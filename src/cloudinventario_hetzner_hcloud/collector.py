import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

from hcloud import Client

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorHetznerHCloud(name, config, defaults, options)

class CloudCollectorHetznerHCloud(CloudCollector):

  COLLECTOR_PKG = "cloudinventario_hetzner_hcloud"

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
      res.append(self.__to_dict(server))
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

  def _logout(self):
    self.client = None
