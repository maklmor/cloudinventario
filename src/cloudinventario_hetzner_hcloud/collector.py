
import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint
from boto3 import resources

from hcloud import Client
from httplib2 import Response

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorHetznerHCloud(name, config, defaults, options)

class CloudCollectorHetznerHCloud(CloudCollector):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)


  def _login(self):
    api_token = self.config['api_token']

    logging.info("logging in hcloud={}".format(self.name))
    self.client = Client(token=api_token)
    return True

  def _fetch(self, collect):
    res = []
    servers = self.client.servers.get_all()
    for server in servers:
      res.append(self._process_vm(server))
      time.sleep(1/4)
    return res

  def __to_dict(self, obj, key = None, level = 0):
    result = {}
    level += 1
    # ignore list
    if key in ['server_types', 'prices', 'servers'] or level > 10:
       return None
    if hasattr(obj, '__slots__') and len(obj.__slots__) > 0:
       for key in obj.__slots__:
         result[key[0].lower() + key[1:]] = self.__to_dict(getattr(obj, key), key, level)
    elif isinstance(obj, list):
       result = []
       for rec in obj:
         result.append(self.__to_dict(rec, key, level))
    else:
       return obj
    return result
    
  def _process_vm(self, server):

    data = self.__to_dict(server)

    networks = []
#    if data["public_net"]:
#       networks.append({
#         "name": "public",
#         "ip": data["public_net"]["ipv4"]["ip"].
#       })
#    for iface in data["private_net"]:
#       ....

    #instance_type = data["InstanceType"]
    #instance_def = self._get_instance_type(instance_type)
    memory_size = data["server_type"]["memory"]*1024
    memory_size = int(memory_size)
    storage_size = data["server_type"]["disk"]*1024
    storage_size = int(storage_size)

    pprint(data)
    vm_data = {
            "id": data["id"],
            "name": data["name"],
            "primary_ip": data["public_net"]["ipv4"]["ip"],
            #"mac_address": None["private_net"]["mac_address"],
            "status": data["status"],
            "is_on": (data["status"] == "running"),
            "cpus": data["server_type"]["cores"],
            "cputype": data["server_type"]["cpu_type"],
            "memory": memory_size,
            "networks": networks,
            "storage": storage_size,
            "storage_type": data["server_type"]["storage_type"],
            "os": data["image"]["os_flavor"],
            "cluster": data["datacenter"]["name"],
            "cluster_name": data["datacenter"]["description"],
            #"server_name": data["datacenter"]["name"],
            "server_type": data["server_type"]["name"],
            "server_location": data["datacenter"]["location"],
            #"server_prices": data["server_type"]["prices"],
            "server_volumes": data["volumes"],
    }


    #print("==================================")
    #pprint(vm_data)
    return self.new_record('vm', vm_data, data)

  def logout(self):
      self.client = None
