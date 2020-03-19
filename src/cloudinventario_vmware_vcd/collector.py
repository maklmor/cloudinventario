import logging, re
from pprint import pprint

import pyvcloud.vcd.client as vcd
from pyvcloud.vcd.org import Org as vcdOrg
from pyvcloud.vcd.vdc import VDC as vcdVDC
from pyvcloud.vcd.vapp import VApp as vcdVApp
from pyvcloud.vcd.vapp import VM as vcdVM
from pyvcloud.vcd.utils import to_dict, vapp_to_dict, vm_to_dict

from cloudinventario.helpers import CloudCollector

def setup(name, config, options):
  return CloudCollectorVMWareVCD(name, config, options)

class CloudCollectorVMWareVCD(CloudCollector):

  def __init__(self, name, config, options):
    super().__init__(name, config, options)

    self.client = None
    self.org = None
    self.vdcName = None

  def _login(self):
    host = self.config['host']
    user = self.config['user']
    passwd = self.config['pass']
    org = self.config['org']
    vdc = self.config.get('vdc')

    logging.info("logging in host={}".format(host))
    self.client = vcd.Client(host, api_version = '29.0',
                             verify_ssl_certs = self.verify_ssl,
                             log_file='/dev/null',
                             log_requests=False,
                             log_headers=False,
                             log_bodies=False)

    # suppress logging
    for name in ['urllib3.connectionpool', 'vcd_pysdk.log']:
      vcd_logger = logging.getLogger(name)
      vcd_logger.setLevel(logging.WARNING)

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
    for vdc_def in vdc_list:
      vdc_name = vdc_def["name"]
      vdc_res = self.org.get_vdc(vdc_name)
      vdc = vcdVDC(self.client, resource=vdc_res)

      res.extend(self.__process_vdc(org_name, vdc_name, vdc))
    return res

  def __process_vdc(self, org_name, vdc_name, vdc):
    res = []
    res_list = vdc.list_resources(vcd.EntityType.VAPP)
    for vapp_def in res_list:
      vapp_name = vapp_def["name"]
      vapp_res = vdc.get_vapp(vapp_name)
      vapp = vcdVApp(self.client, resource=vapp_res)
      res.extend(self.__process_vapp(org_name, vdc_name, vapp_name, vdc, vapp))
    return res

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
    disk_re = re.compile("^disk-")
    resource_type = vcd.ResourceType.VM.value
    vm_list = vdc.list_vapp_details(resource_type, 'containerName==' + vapp_name)
    for vm_def in vm_list:
      rec = to_dict(vm_def, resource_type=resource_type)
      vm_name = rec["name"]
      vm_res = vapp.get_vm(vm_name)
      vm = vcdVM(self.client, href=vm_res.get('href'))

      rec_detail = self.__process_vm(org_name, vdc_name, vapp_name, vm_name, vdc, vapp, vm)
      rec = {**rec, **rec_detail}

      logging.debug("new VM name={}".format(rec["name"]))
      res.append(self.new_record('vm', {
        "created": rec["DateCreated"],
        "name": rec["name"],
        "project": rec["vappName"],
        "description": rec["Description"],
        "id": rec["id"],
        "cpus": int(rec.get("numberOfCpus") or 0),
        "memory": int(rec.get("memoryMB") or 0),
        "disks": len(list(filter(disk_re.match, rec.keys()))),
        "storage": sum(int(rec[key]["size-MB"]) for key in list(filter(disk_re.match, rec.keys()))),
        "primary_ip": rec["ipAddress"],
        "networks": None,	# TODO !!!
        "os": rec["guestOs"],
        "status": rec["status"],
        "is_on": (vm.is_powered_on() and 1 or 0),
        "owner": rec["ownerName"]
      }, rec))
    return res

  def __process_vm(self, org_name, vdc_name, vapp_name, vm_name, vdc, vapp, vm):
    # VM details
    rec = to_dict(vm.get_resource())
    rec_detail = vm_to_dict(vm.get_resource())
    rec = {**rec, **rec_detail}
    rec["orgName"] = org_name
    rec["vdcName"] = vdc_name
    rec["vappName"] = vapp_name
    rec["vapp"] = vapp_name
    return rec

  def _logout(self):
    self.client.logout()
