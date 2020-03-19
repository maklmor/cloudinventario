"""CloudInventario"""
import yaml, sys, importlib, re
from pprint import pprint

from cloudinventario.storage import InventoryStorage

COLLECTOR_PREFIX = 'cloudinventario'

class CloudInventario:

   def __init__(self, config_file):
     self.config = self.load_config(config_file)
     self._inventory = []

   def load_config(self, config_file):
     with open(config_file) as file:
       return yaml.safe_load(file)

   @property
   def collectors(self):
     return self.config['collectors'].keys()

   def collectorConfig(self, collector):
     return self.config['collectors'][collector]

   def loadCollector(self, collector, options = None):
     mod_cfg = self.collectorConfig(collector)

     mod_name = mod_cfg['module']
     mod_name = re.sub(r'[/.]', '_', mod_name) # basic safety, should throw error
     mod_name = re.sub(r'_', '__', mod_name)
     mod_name = re.sub(r'-', '_', mod_name)

     mod = importlib.import_module(COLLECTOR_PREFIX + '_' + mod_name + '.collector')
     mod_instance = mod.setup(collector, mod_cfg['config'], options or {})
     return mod_instance

   def collect(self, collector):
     instance = self.loadCollector(collector)

     instance.login()
     self._inventory = instance.fetch()
     instance.logout()

     return True

   @property
   def inventory():
     return self._inventory

   def store(self):
     store_config = self.config["storage"]
     store = InventoryStorage(store_config)

     store.connect()
     store.save(self._inventory)
     store.disconnect()

     return True
