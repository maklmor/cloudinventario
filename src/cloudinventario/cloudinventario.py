"""CloudInventario"""
import os, sys, importlib, re, threading, logging
from pprint import pprint

from cloudinventario.storage import InventoryStorage

COLLECTOR_PREFIX = 'cloudinventario'

class CloudInventario:

   def __init__(self, config):
     self.config = config
     self.lock = threading.Lock()

   @property
   def collectors(self):
     collectors = []
     for col in self.config['collectors'].keys():
        if self.config['collectors'][col].get("disabled") != True:
          collectors.append(col)
     return collectors

   @property
   def expiredCollectors(self):
     # TODO
     pass

   def collectorConfig(self, collector):
     return self.config['collectors'][collector]

   def loadCollector(self, collector, options = None):
     mod_cfg = self.collectorConfig(collector)

     mod_name = mod_cfg['module']
     mod_config = mod_cfg['config']
     mod_defaults = mod_cfg.get('default', {})
     return CloudInventario.loadCollectorModule(mod_name, collector, mod_config, mod_defaults, options)

   @staticmethod
   def loadCollectorModule(mod_name, collector, config, defaults = None, options = None):
     mod_name = re.sub(r'[/.]', '_', mod_name) # basic safety, should throw error
     mod_name = re.sub(r'_', '__', mod_name)
     mod_name = re.sub(r'-', '_', mod_name)
     mod_pkg = COLLECTOR_PREFIX + '_' + mod_name

     mod = importlib.import_module(mod_pkg + '.collector')
     mod_instance = mod.setup(collector, config, defaults, options or {})

     # XXX: init for resource collectors (I don't like it)
     mod_instance._init(
        collector_pkg = mod_pkg,
        resources = config.get('collect', [])
     )
     return mod_instance

   def collect(self, collector, options = None):
     # workaround for buggy libs
     wd = os.getcwd()
     os.chdir("/tmp")

     inventory = None
     try:
       instance = self.loadCollector(collector, options)
       instance.login()
       inventory = instance.fetch()
       instance.logout()
     except Exception as e:
       logging.error("Exception while processing collector={}".format(collector))
       raise
     finally:
       os.chdir(wd)
     return inventory

   def store(self, inventory, runtime = None):
     store_config = self.config["storage"]

     with self.lock:
       store = InventoryStorage(store_config)

       store.connect()
       store.save(inventory, runtime)
       store.disconnect()

     return True

   def store_status(self, source, status, runtime = None, error = None):
     store_config = self.config["storage"]

     with self.lock:
       store = InventoryStorage(store_config)
       store.connect()
       store.log_status(source, status, runtime, error)
       store.disconnect()
     return True

   def cleanup(self, days):
     store_config = self.config["storage"]
     store = InventoryStorage(store_config)

     store.connect()
     store.cleanup(days)
     store.disconnect()
