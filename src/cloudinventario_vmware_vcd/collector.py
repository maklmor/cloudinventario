import concurrent.futures
import logging, re, sys, asyncio
from pprint import pprint

import pyvcloud.vcd.client as vcd
from pyvcloud.vcd.org import Org as vcdOrg
from pyvcloud.vcd.vdc import VDC as vcdVDC
from pyvcloud.vcd.vapp import VApp as vcdVApp
from pyvcloud.vcd.vapp import VM as vcdVM
from pyvcloud.vcd.utils import to_dict, vapp_to_dict, vm_to_dict, stdout_xml
from pyvcloud.vcd.client import QueryResultFormat
from pyvcloud.vcd.client import ResourceType

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorVMWareVCD(name, config, defaults, options)

class CloudCollectorVMWareVCD(CloudCollector):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)

    self.client = None
    self.org = None
    self.vdcName = None

  def _login(self):
    host = self.config['host']
    user = self.config['user']
    passwd = self.config['pass']
    org = self.config['org']
    vdc = self.config.get('vdc')

    # suppress logging
    for name in ['urllib3.connectionpool', 'vcd_pysdk.log']:
      vcd_logger = logging.getLogger(name)
      vcd_logger.propagate = False
      vcd_logger.setLevel(logging.WARNING)

    logging.info("logging in host={}".format(host))
    self.client = vcd.Client(host, api_version = '29.0',
                             verify_ssl_certs = self.verify_ssl,
                             log_file='log',
                             log_requests=False,
                             log_headers=False,
                             log_bodies=True)

    self.client.set_highest_supported_version()
    self.client.set_credentials(vcd.BasicLoginCredentials(user, org, passwd))
    # TODO: check logged in ?

    org_res = self.client.get_org()
    self.org = vcdOrg(self.client, resource = org_res)
    logging.info("logged in")

    self.vdcName = vdc
    return True

  def _fetch(self, collect):
    vdc_list = None
    if self.vdcName:
      vdc_list = [ { "name": self.vdcName } ]
    else:
      vdc_list = self.org.list_vdcs()

    res = []
    org_name = self.org.get_name()


    rec = None
    res.append(self.new_record('org', {
      "name": org_name,
    }, rec))

    for vdc_def in vdc_list:
      vdc_name = vdc_def["name"]
      vdc_res = self.org.get_vdc(vdc_name)
      vdc = vcdVDC(self.client, resource=vdc_res)

      res.extend(self.__process_vdc(org_name, vdc_name, vdc))
    return res

  def __process_vdc(self, org_name, vdc_name, vdc):
    res = []

    vdc.get_resource()
    rec = self.__to_dict(vdc.resource);

    # get more info
    query_filter = ('name', vdc_name)
    query = self.client.get_typed_query(
      ResourceType.ORG_VDC.value,
      query_result_format=QueryResultFormat.RECORDS,
      equality_filter=query_filter)
    result = list(query.execute())
    if len(result) == 1:
      rec_detail = to_dict(result[0])
      rec = {**rec_detail, **rec}
    else:
      logging.warning("Failed to fetch ORG_VDC reconrd for vdc name ={}".format(vdc_name))

    logging.debug("new vdc name={}".format(vdc_name))
    res.append(self.new_record('vdc', {
      "name": vdc_name,
      "cluster": org_name,
      "description": rec.get("description"),
    }, rec))

    res_list = vdc.list_resources(vcd.EntityType.VAPP)
    for vapp_def in res_list:
      vapp_name = vapp_def["name"]
      try:
        vapp_res = vdc.get_vapp(vapp_name)
        vapp = vcdVApp(self.client, resource=vapp_res)
        res.extend(self.__process_vapp(org_name, vdc_name, vapp_name, vdc, vapp))
      except Exception as e:
        logging.exception("Exception while processing VApp = {}".format(vapp_name))
        pass
      if TEST:
        break
    return res

  def __process_vmlist_vm(self, org_name, vdc_name, vapp_name, vdc, vapp, vm_def, resource_type):
    disk_re = re.compile("^disk-")
    nic_re = re.compile("^nic-")

    rec = to_dict(vm_def, resource_type=resource_type)
    vm_name = rec["name"]
    vm_res = vapp.get_vm(vm_name)
    vm = vcdVM(self.client, href=vm_res.get('href'))

    rec_detail = self.__process_vm(org_name, vdc_name, vapp_name, vm_name, vdc, vapp, vm)
    rec = {**rec, **rec_detail}

    networks = []
    primary_ip = rec.get("ipAddress")
    for key in list(filter(nic_re.match, rec.keys())):
      net = {
        "id": key,
        "name": rec[key].get("name") or key,
        "mac": rec[key].get("mac"),
        "ip": rec[key].get("ip"),
        "network": rec[key].get("network"),
        "connected": (rec[key].get("connected") == "true" and True or False)
      }
      if net["ip"] == rec.get("ipAddress"):
        net["primary"] = True
      if key == rec.get('primary-nic'):
        primary_ip = net["ip"]
      networks.append(net)
    if len(networks) > 0:
      rec["networks"] = networks

    storages = []
    storage_size = 0
    for key in list(filter(disk_re.match, rec.keys())):
      storage_size += int(rec[key].get("size-MB"));
      disk = {
        "id": rec[key].get("id") or key,
        "name": rec[key].get("name") or key,
        "capacity": int(rec[key].get("size-MB") or 0),
        "free": None,
        "profile": rec[key].get("storageProfile") and rec[key].get("storageProfile").get("name"),
        "thin": rec[key].get("ThinProvisioned"),
        "ssd": None
      }
      storages.append(disk)
    if len(storages) > 0:
      rec["storages"] = storages

    logging.debug("new VM name={}".format(rec["name"]))
    return self.new_record('vm', {
      "created": rec["DateCreated"],
      "name": rec["name"],
      "cluster": rec["vdcName"],
      "project": rec["vappName"],
      "description": rec.get("Description"),
      "id": rec["id"],
      "cpus": int(rec.get("numberOfCpus") or 0),
      "memory": int(rec.get("memoryMB") or 0),
      "disks": len(storages),
      "storage": storage_size, #sum(int(rec[key]["size-MB"]) for key in list(filter(disk_re.match, rec.keys()))),
      "primary_ip": primary_ip,
      "networks": rec.get("networks"),
      "storages": rec.get("storages"),
      "os": rec["guestOs"],
      "status": rec["status"],
      "is_on": (vm.is_powered_on() and 1 or 0),
      "owner": rec["ownerName"]
    }, rec)

  def __process_vapp(self, org_name, vdc_name, vapp_name, vdc, vapp):
    res = []

    # VApp details
    resource_type = vcd.ResourceType.VAPP.value
    vapp_list = vdc.list_vapp_details(resource_type, 'name==' + vapp_name)

    rec = to_dict(vapp_list[0])
    rec["orgName"] = org_name

    logging.debug("new vapp name={}".format(rec["name"]))
    res.append(self.new_record('vapp', {
      "created": rec["creationDate"],
      "name": rec["name"],
      "id": rec["name"],	# TODO: should be smth. better
      "cluster": rec["vdcName"],
      "project": rec["vdcName"],
      "cpus": int(rec.get("numberOfCpus") or 0),
      "memory": int(rec.get("memoryAllocationMB") or 0),
      "storage": int(rec.get("storageKB") or 0) // 1024,
      "os": "VMWare VApp",
      "os_family": None,
      "status": rec["status"],
      "is_on": (vapp.is_powered_on() and 1 or 0),
      "owner": rec["ownerName"]
    }, rec))

    # process VMs
    resource_type = vcd.ResourceType.VM.value
    try:
      vm_list = vdc.list_vapp_details(resource_type, 'containerName==' + vapp_name)
    except:
      logging.error("failed to get VM list for vapp={}".format(vapp_name))
      return [res]

    with concurrent.futures.ThreadPoolExecutor(max_workers = self.options["tasks"] or 1) as executor:
      futures = []
      for vm_def in vm_list:
         futures.append(executor.submit(self.__process_vmlist_vm, org_name, vdc_name, vapp_name, vdc, vapp, vm_def, resource_type))
      for future in concurrent.futures.as_completed(futures):
         res.append(future.result())
    return res

  def __process_vm(self, org_name, vdc_name, vapp_name, vm_name, vdc, vapp, vm):
    # VM details
    vm.get_resource();
    rec = to_dict(vm.resource)
    rec_detail = vm_to_dict(vm.resource)
    rec = {**rec, **rec_detail}
    # special handling for disks
    for disk in vm.resource.VmSpecSection.DiskSection.DiskSettings:
      rec_disk = self.__to_dict(disk)
      rec_disk["id"] = str(disk["DiskId"])
      rec_disk_key = "disk-" + rec_disk["id"]
      rec[rec_disk_key] = {**rec[rec_disk_key], **rec_disk}

    rec["orgName"] = org_name
    rec["vdcName"] = vdc_name
    rec["vappName"] = vapp_name
    rec["vapp"] = vapp_name
    return rec

  def __to_dict(self, obj):
    result = {}
    if hasattr(obj, '__dict__') and len(obj.__dict__) > 0:
       # XXX: not handling iterable objects (not possible without knowing struct)
       for key in obj.__dict__:
         result[key[0].lower() + key[1:]] = self.__to_dict(obj[key])
    elif hasattr(obj, 'keys') and len(obj.keys()) > 0:
       for key in obj.keys():
         if key not in ['type']:
           result[key[0].lower() + key[1:]] = obj.get(key)
    else:
       return obj.text
    return result

  def _logout(self):
    self.client.logout()
