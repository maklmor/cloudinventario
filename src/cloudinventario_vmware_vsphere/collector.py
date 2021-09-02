import concurrent.futures
import sys, logging, re
from pprint import pprint

import ssl
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

from cloudinventario.helpers import CloudCollector

# TEST mode enabled (limits number of fetched items)
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorVMWareVSphere(name, config, defaults, options)

class CloudCollectorVMWareVSphere(CloudCollector):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)

    self.client = None
    self.maxDepth = 10
    self.vm2cluster = {}
    self.vm2dvsPort = {}
    self.vm2dvsPortKey = {}

  def _login(self):
    host = self.config['host']
    port = self.config.get('port', 443)
    user = self.config['user']
    passwd = self.config['pass']

    context = None
    if not self.verify_ssl:
      if hasattr(ssl, '_create_unverified_context'):
        context = ssl._create_unverified_context()
    logging.info("logging in host={}, user={}".format(host, user))
    try:
      self.client = SmartConnect(host=host,
                               user=user,
                               pwd=passwd,
                               port=port,
                               sslContext=context)
    except:
      logging.error("failed to log in to host={}".format(host))
      return False
    if not self.client:
      return False
    logging.info("logged in")
    return True

  def _fetch(self, collect):
    res = []

    self.content = self.client.RetrieveContent()

    # collect networks (DistributedVirtualPortgroup)
    logging.info("collecting networks")
    self.networks = {}
    dvs_portgroup = None
    for child in self.content.rootFolder.childEntity:
      if hasattr(child, 'network'):
        for net in child.network:
          if isinstance(net, vim.DistributedVirtualPortgroup):
            logging.debug("new network key={} name={}".format(net.key, net.name))
            self.networks[net.key] = net.name
            dvp_net = net
          elif isinstance(net, vim.Network):
            logging.debug("new network name={}".format(net.name))
            self.networks[net.name] = net.name
          else:
            logging.error("bad network name={} type={}".format(net.name, net))	# TODO: handle this !

    # fetch ports
    try:
      ports = net.config.distributedVirtualSwitch.FetchDVPorts()
      for port in ports:
        if port.connectee and port.connectee.connectedEntity and port.state:
           vmid = port.connectee.connectedEntity._moId
           state = port.state.runtimeInfo

           if not vmid in self.vm2dvsPort:
             self.vm2dvsPort[vmid] = []
             self.vm2dvsPortKey[vmid] = []
           try:
             if port.key not in self.vm2dvsPortKey[vmid]:
               self.vm2dvsPortKey[vmid].append(port.key)
               rec = {
                 "portKey": port.key,
                 "nicKey": port.connectee.nicKey,
                 "connected": state.linkUp,
                 "vlan": state.vlanIds[0].start,
                 #"vlanRange": [ state.vlanIds.start, state.vlanIds.end ],
                 "mac": state.macAddress,
                 "portgroup": port.portgroupKey,
                 "network": self.networks[port.portgroupKey],
                 "dvsUUID": port.dvsUuid,
               }
               try:
                 rec["idx"] = int(re.split('\.(eth|vmnic)', state.linkPeer, 1)[1])
                 rec["ethName"] = state.linkPeer.split('.').pop()
               except:
                 pass

               self.vm2dvsPort[vmid].append(rec)
           except:
             pass

      # sort by idx
      for vmid in self.vm2dvsPort:
        self.vm2dvsPort[vmid] = sorted(self.vm2dvsPort[vmid], key = lambda kv: kv.get('idx', 0))
    except:
      pass
    #pprint(self.vm2dvsPort)

    # collect hosts
    logging.info("collecting clusters")
    for child in self.content.rootFolder.childEntity:
      if hasattr(child, 'hostFolder'):
        for cluster in child.hostFolder.childEntity:
          if isinstance(cluster, vim.ComputeResource):
            recs = self.__process_cluster(cluster)
            if recs:
              res.extend(recs)
            if TEST:
              break
          else:
            logging.error("bad compute resource name={}".format(cluster.name))

    # collect VMs
    logging.info("collecting VApps and VMs")
    for child in self.content.rootFolder.childEntity:
      #print("child = {}".format(child.name))

      if hasattr(child, 'vmFolder'):
        datacenter = child
        vmFolder = datacenter.vmFolder
        with concurrent.futures.ThreadPoolExecutor(max_workers = self.options["tasks"] or 1) as executor:
          futures = []
          for vm in vmFolder.childEntity:
            futures.append(executor.submit(self.__process_vmchild, vm))

          for future in concurrent.futures.as_completed(futures):
            recs = future.result()
            if recs:
              res.extend(recs)

    return res

  def __process_cluster(self, cluster):
    name = cluster.name
    logging.debug("new cluster name={}".format(name))

    # collect virtual machines
    obj_view = self.content.viewManager.CreateContainerView(cluster, [vim.VirtualMachine], True)
    vm_list = obj_view.view
    obj_view.Destroy()
    for vm in vm_list:
      self.vm2cluster[vm._moId] = name

    # collect cluster data
    cs = cluster.summary
    rec = {
    	"name": name,
    	"id": cluster._moId,
    	"cpus": cs.numCpuCores,
    	"threads": cs.numCpuThreads,
    	"hosts": cs.numHosts,
    	"memory": cs.totalMemory // (1024 * 1024)
    }

    res = []
    res.append(self.new_record('cluster', {
      "name": rec["name"],
      "id": rec["id"],
      "cpus": rec["cpus"],
      "memory": rec["memory"]
    }, rec))

    logging.info("collecting cluster hosts")
    for host in cluster.host:
      res.extend(self.__process_host(host))
      if TEST:
        break

    return res

  def __process_host(self, host):
    # ignore host not connected
    if host.runtime.connectionState != 'connected':
      return []

    name = host.name
    logging.debug("new server name={}".format(name))
    hs = host.summary
    hp = hs.config.product
    hcpu = host.hardware.cpuPkg[0]
    rec = {
      "name": name,
      "id": host._moId,
      "management_ip": hs.managementServerIp,
      "primary_ip": None,
      "memory": hs.hardware.memorySize // (1024 * 1024),
      "cpus": hs.hardware.numCpuCores,
      "cpu_vendor": hcpu.vendor,
      "cpu_description": hcpu.description,
      "threads": hs.hardware.numCpuThreads,
      "nics": hs.hardware.numNics,
      "UUID": hs.hardware.uuid,
      "hw_vendor": hs.hardware.vendor,
      "hw_model": hs.hardware.model,
      "is_maintanance": hs.runtime.inMaintenanceMode,
      "status": hs.runtime.powerState,
      "is_on" : (hs.runtime.powerState == "poweredOn" and 1 or 0),
      "os": hp.fullName,
      "sw_license": hp.licenseProductName,
      "os_type": hp.osType,
      "sw_name": hp.name,
      "sw_vendor": hp.vendor,
      "sw_version": hp.version,
      "cluster": host.parent.name,
      "storage": None,
      "networks": None,
      "storages": None
    }

    # storage
    storage = 0
    storage_free = 0
    datastore = []
    for ds in host.datastore:
      storage += ds.summary.capacity
      storage_free += ds.summary.freeSpace
      info = {
        "name": ds.summary.name,
        "capacity": ds.summary.capacity // (1024 * 1024),
        "free": ds.summary.freeSpace // (1024 * 1024),
        "ssd": None
      }
      if hasattr(ds.info, 'vmfs'):
        info["ssd"] = ds.info.vmfs.ssd
      datastore.append(info)
    if storage > 0:
      rec["storage"] = storage // (1024 * 1024)
      rec["storages"] = datastore

    # networks (
    networks = []
    netdevinfo = {}
    for nic in host.config.network.vnic:
      netdevinfo[nic.spec.mac] = {
        "ip": nic.spec.ip.ipAddress or None,
        "subnet": nic.spec.ip.subnetMask or None
      }
    for nic in host.config.network.pnic:
      net = {
        "name": nic.device,
        "mac": nic.mac,
        "ip": netdevinfo.get(nic.mac, {}).get("ip") or (nic.spec.ip.ipAddress or None),
        "subnet": netdevinfo.get(nic.mac, {}).get("subnet")
      }
      if not rec["primary_ip"]:
         rec["primary_ip"] = net["ip"]
      if net["ip"] and net["ip"] == rec["primary_ip"]:
        net["primary"] = True
      networks.append(net)
    if len(networks) > 0:
      rec["networks"] = networks

    res = []
    res.append(self.new_record("server", {
        "name": rec["name"],
        "project": rec["cluster"],
        "id": rec["id"],
        "cpus": int(rec.get("cpus") or 0),
        "memory": int(rec.get("memory") or 0),
        "storage": int(rec.get("storage") or 0),
        "storages": rec.get("storages"),
        "primary_ip": rec["primary_ip"],
        "management_ip": rec["management_ip"],
        "networks": rec["networks"],
        "os": rec["os"],
        "status": rec["status"],
        "is_on": rec["is_on"],
    }, rec))
    return res

  def __process_vmchild(self, child, depth = 1, prefix = None):

    res = []
    if isinstance(child, vim.Folder) or isinstance(child, vim.VirtualApp):
      if prefix == None:
        prefix = child.name
      else:
        prefix = prefix + '.' + child.name
      res.extend(self.__process_vapp(child, prefix))
    #print("vmchild = {}/{}".format(child.name, type(child).__name__))

    # if this is a group it will have children. if it does, recurse into them
    # and then return
    if hasattr(child, 'childEntity'):
      if depth > self.maxDepth:
        return []

      for c in child.childEntity:
        res.extend(self.__process_vmchild(c, depth + 1, prefix))
        if TEST:
          break
      return res

    # if this is a vApp, it likely contains child VMs
    # (vApps can nest vApps, but it is hardly a common usecase, so ignore that)
    if isinstance(child, vim.VirtualApp):
      vmList = vm.vm

      res.extend(self.__process_vapp(child, prefix))

      for c in vmList:
        res.extend(self.__process_vmchild(c, depth + 1, prefix))
        if TEST:
          break
        break
      return res

    if isinstance(child, vim.VirtualMachine):
      res.extend(self.__process_vm(child, prefix))
    return res

  def __process_vapp(self, vapp, name):
    logging.debug("new vapp name={}".format(name))

    vs = None
    if isinstance(vapp, vim.VirtualApp):
      vs = vapp.summary
    rec = {
      "name": name,
      "id": vapp._moId,
      "memory": (vs and vs.configuredMemoryMB or 0)
    }

    res = []
    res.append(self.new_record('vapp', {
      "name": rec["name"],
      "id": rec["id"],
      "memory": (rec.get("memory") or 0)
    }, rec))
    return res

  def __process_vm(self, vm, parent):
    name = vm.name
    logging.debug("new vm name={}".format(name))
    vs = vm.summary

    vs_runtime_host_name = None
    vs_runtime_cluster_name = None
    try:
      vs_runtime_host_name = vm.runtime.host.summary.name
      vs_runtime_cluster_name = vs.runtime.host.parent.name
    except:
      pass

    rec = {
      "name": vm.name,
      "config_name": vs.config.name,
      "os": vs.config.guestFullName,
      "guest_id": vs.config.guestId,
      "description": vs.config.annotation,
      "cpus": vm.config.hardware.numCPU,
      "memory": vm.config.hardware.memoryMB,
      "disks": vs.config.numVirtualDisks,
      "id": vm._moId,
      "instanceUUID": vs.config.instanceUuid,
      "UUID": vs.config.uuid,
      "storage": None,
      "primary_ip": ((vs.guest.ipAddress and ":" not in vs.guest.ipAddress) and vs.guest.ipAddress or None), # no ipv6 here
      "status": vs.runtime.powerState,
      "is_on": (vs.runtime.powerState == "poweredOn" and 1 or 0),
      "host": vs_runtime_host_name,
      "project": parent,
      "vapp": parent,
      "datastore": [ ds.datastore.name for ds in vm.storage.perDatastoreUsage ],
      "cluster": vs_runtime_cluster_name or self.vm2cluster.get(vm._moId),
      #"management_ip": vs.runtime.host.parent.summary.managementServerIp	# TODO: need this
      #"tags": vm.tags,
    }

    if vs.config.template:
      rec['template'] = 1

    # storage
    storage = 0
    storages = []
    for vd in vm.config.hardware.device:
      if isinstance(vd, vim.VirtualDisk):
        storage += vd.capacityInKB
        datastore = None
        thin = None

        # get attribs
        if isinstance(vd.backing, vim.vm.device.VirtualDevice.FileBackingInfo):
          datastore = vd.backing.datastore.name
        elif isinstance(vd.backing, vim.vm.device.VirtualDisk.FlatVer2BackingInfo):
          datastore = vd.backing.deviceName
        if isinstance(vd.backing, vim.vm.device.VirtualDisk.FlatVer2BackingInfo):
          thin = vd.backing.thinProvisioned

        # TODO: add info from vm.guest.disk
        storages.append({
          "id": vd.key,
          "name": vd.deviceInfo.label,
          "capacity": int(vd.capacityInKB // 1024),
          "free": None,
          "profile": datastore,
          "thin": thin,
          "ssd": None,
        })
    if storage > 0:
      rec["storage"] = int(storage // 1024)
    if len(storages) > 0:
      rec["storages"] = storages

    # networks
    networks = []
    #pprint(vm.config.hardware.device)
    #pprint(vm.guest.net)
    for nic_idx in range(len(vm.guest.net)):
      nic = vm.guest.net[nic_idx]
      net = {
        "id": nic.deviceConfigId,
        "mac": nic.macAddress,
        "network": nic.network,
        "ip": None,
        "connected": nic.connected
      }

      if nic.ipConfig:
        for ip in nic.ipConfig.ipAddress:
          if not net["ip"] and ip.prefixLength <= 32:	# DUMMY distinguish IPv4 address
            net["ip"] = ip.ipAddress
            if ip.prefixLength != 0:
              net["prefix"] = ip.prefixLength
            if not rec["primary_ip"]:
              rec["primary_ip"] = net["ip"]
          else:
            if not net.get("aliases"):
              net["aliases"] = []
            net["aliases"].append(ip.ipAddress + "/" + str(ip.prefixLength or 32))
        if net["ip"] and net["ip"] == rec["primary_ip"]:
          net["primary"] = True

      # XXX: Cisco fix
      #   vmguest is not reporting correct data, we have to map config
      #   to reported IPs (this is a big heuristic)
      if len(vm.guest.net) == 1 and \
          net['id'] <= 0 and net['network'] is None and \
          net['mac'] == '00:11:22:33:44:55' and \
          net['ip'] and (len(self.vm2dvsPort[rec['id']]) == 1 + len(net.get('aliases', []))):
        ports = self.vm2dvsPort[rec['id']]
        portIP = [ net['ip'] ]
        portIP.extend(net.get('aliases', []))

        net.pop('aliases', None)
        for idx in range(len(ports)):
          port = ports[idx]
          net["id"] = port["nicKey"]
          net["mac"] = port["mac"]
          net["network"] = port["network"]
          net["ip"] = portIP[idx].split('/', 1)[0]
          net["connected"] = port["connected"]
          net["__fix"] = 'ip2port-multi'
          networks.append(net)
          net = {}
      elif net['id'] <= 0 and net['network'] is None and net['ip'] and rec['id'] in self.vm2dvsPort:
        ports = self.vm2dvsPort[rec['id']]
        fixed = False
        for port in ports:
           if port['mac'] == net['mac']:
             net["id"] = port["nicKey"]
             net["network"] = port["network"]
             net["__fix"] = "mac2port"
             fixed = True
             break
        if not fixed:
          net["id"] = 'CI-NIC' + str(nic_idx)
          net["name"] = 'nic' + str(nic_idx)
          net["__fix"] = "autoname"
        networks.append(net)
      elif net['ip']:
        networks.append(net)
    if len(networks) > 0:
      rec["networks"] = networks

    res = []
    res.append(self.new_record('vm', {
      "name": rec["name"],
      "cluster": rec["cluster"],
      "project": rec["vapp"],
      "description": rec["description"],
      "id": rec["id"],
      "cpus": int(rec.get("cpus") or 0),
      "memory": int(rec.get("memory") or 0),
      "disks": int(rec.get("disks") or 0),
      "storage": int(rec.get("storage") or 0),
      "storages": rec.get("storages"),
      "primary_ip": rec["primary_ip"],
      "networks": rec.get("networks"),
      "os": rec["os"],
      "status": rec["status"],
      "is_on": rec["is_on"],
      "template": int(rec.get("template") or 0)
    }, rec))
    return res

  def _logout(self):
    Disconnect(self.client)
