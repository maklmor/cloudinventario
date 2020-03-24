import sys, logging
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
    self.client = SmartConnect(host=host,
                               user=user,
                               pwd=passwd,
                               port=port,
                               sslContext=context)
    if not self.client:
      return False
    logging.info("logged in")
    return True

  def _fetch(self, collect):
    res = []

    content = self.client.RetrieveContent()

    # collect networks (DistributedVirtualPortgroup)
    logging.info("collecting networks")
    self.networks = {}
    for child in content.rootFolder.childEntity:
      if hasattr(child, 'network'):
        for net in child.network:
          if isinstance(net, vim.DistributedVirtualPortgroup):
            logging.debug("new network name={}".format(net.name))
            self.networks[net.key] = net.name
          else:
            logging.error("bad network name={}".format(net.name))	# TODO: handle this !

    # collect hosts
    logging.info("collecting clusters")
    for child in content.rootFolder.childEntity:
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
    for child in content.rootFolder.childEntity:
      if hasattr(child, 'vmFolder'):
        datacenter = child
        vmFolder = datacenter.vmFolder
        for vm in vmFolder.childEntity:
          recs = self.__process_vmchild(vm)
          if recs:
            res.extend(recs)
    return res

  def __process_cluster(self, cluster):
    name = cluster.name
    logging.debug("new cluster name={}".format(name))
    cs = cluster.summary
    rec = {
    	"name": name,
    	"id": cluster._moId,
    	"cpus": cs.numCpuCores,
    	"threads": cs.numCpuThreads,
    	"hosts": cs.numCpuThreads,
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
    rec = {
      "name": name,
      "id": host._moId,
      "management_ip": hs.managementServerIp,
      "memory": hs.hardware.memorySize // (1024 * 1024),
      "cpus": hs.hardware.numCpuCores,
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
    }

    storage = 0
    for ds in host.datastore:
      storage += ds.summary.capacity
    if storage > 0:
      rec["storage"] = storage // (1024 * 1024)

    # networks
    networks = []
    for nic in host.config.network.pnic:
      networks.append({
        "name": nic.device,
        "mac": nic.mac,
        "ip": nic.spec.ip.ipAddress
      })
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
        "primary_ip": None,
        "management_ip": rec["management_ip"],
        "networks": rec["networks"],
        "os": rec["os"],
        "status": rec["status"],
        "is_on": rec["is_on"],
    }, rec))
    return res

  def __process_vmchild(self, child, depth = 1):
    # if this is a group it will have children. if it does, recurse into them
    # and then return
    if hasattr(child, 'childEntity'):
      if depth > self.maxDepth:
        return []

      res = []
      for c in child.childEntity:
        res.extend(self.__process_vmchild(c, depth + 1))
        if TEST:
          break
      return res

    # if this is a vApp, it likely contains child VMs
    # (vApps can nest vApps, but it is hardly a common usecase, so ignore that)
    if isinstance(child, vim.VirtualApp):
      vmList = vm.vm

      res = []
      for c in vmList:
        res.extend(self.__process_vmchild(c, depth + 1))
        if TEST:
          break
      return res

    res = []
    if isinstance(child, vim.VirtualMachine):
      res.extend(self.__process_vm(child))
    return res

  def __process_vapp(self, vapp):
    name = vapp.name
    logging.debug("new vapp name={}".format(name))
    vs = vapp.summary
    rec = {
      "name": name,
      "id": vapp._moId,
      "memory": vs.configuredMemoryMB
    }

    res = []
    res.append(self.new_record('vapp', {
      "name": rec["name"],
      "id": rec["id"],
      "memory": (rec.get("memory") or 0)
    }, rec))
    return res

  def __process_vm(self, vm):
    name = vm.name
    logging.debug("new vm name={}".format(name))
    vs = vm.summary
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
      "host": vs.runtime.host.name,
      "project": vm.parent.name,
      "vapp": vm.parent.name,
      "datastore": [ ds.datastore.name for ds in vm.storage.perDatastoreUsage ],
      "cluster": vs.runtime.host.parent.name,
      #"management_ip": vs.runtime.host.parent.summary.managementServerIp	# TODO: need this
      #"tags": vm.tags,
    }

    # storage
    storage = 0
    for vd in vm.config.hardware.device:
      if isinstance(vd, vim.VirtualDisk):
        storage += vd.capacityInKB
    if storage > 0:
      rec["storage"] = storage // 1024

    # networks
    networks = []
    for nic in vm.guest.net:
      net = { "mac": nic.macAddress, "network": nic.network,
        "ip": None, "connected": nic.connected
         }
      for ip in nic.ipConfig.ipAddress:
        if not net["ip"] and ip.prefixLength <= 32:	# DUMMY distinguish IPv4 address
          net["ip"] = ip.ipAddress
          net["prefix"] = ip.prefixLength
          if not rec["primary_ip"]:
            rec["primary_ip"] = net["ip"]
        else:
          if not net.get("aliases"):
            net["aliases"] = []
          net["aliases"].append(ip.ipAddress + "/" + str(ip.prefixLength))
      if net["ip"] == rec["primary_ip"]:
        net["primary"] = True
      networks.append(net)
    if len(networks) > 0:
      rec["networks"] = networks

    res = []
    res.append(self.new_record('vm', {
      "name": rec["name"],
      "project": rec["vapp"],
      "description": rec["description"],
      "id": rec["id"],
      "cpus": int(rec.get("cpus") or 0),
      "memory": int(rec.get("memory") or 0),
      "disks": int(rec.get("disks") or 0),
      "storage": int(rec.get("storage") or 0),
      "primary_ip": rec["primary_ip"],
      "networks": rec.get("networks"),
      "os": rec["os"],
      "status": rec["status"],
      "is_on": rec["is_on"]
    }, rec))
    return res

  def _logout(self):
    Disconnect(self.client)
