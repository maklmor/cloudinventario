"""Classes used by CloudInventario."""
import requests
import datetime
import json
import logging
import importlib
from pprint import pprint

import cloudinventario.platform as platform

class CloudEncoder(json.JSONEncoder):
  def default(self, z):
    if isinstance(z, datetime.datetime):
      return (str(z))
    else:
      return super().default(z)

class CloudCollector:
  """Cloud collector."""

  def __init__(self, name, config, defaults, options):
    self.name = name
    self.config = config
    self.defaults = defaults
    self.options = options
    self.allow_self_signed = options.get('allow_self_signed', config.get('allow_self_signed', False))
    if self.allow_self_signed:
      requests.packages.urllib3.disable_warnings()
    self.verify_ssl = self.options.get('verify_ssl_certs', config.get('verify_ssl_certs', True))
    self.rd = {}  # rd <=> resource_data

  def __pre_request(self):
    pass

  def __post_request(self):
    pass

  def login(self):
    self.__pre_request()
    try:
      res = self._login()
      return res
    except:
      raise
    finally:
      self.__post_request()

  def fetch(self, collect = None):
    self.__pre_request()
    try:
      res = self._fetch(collect)
      return res
    except:
      raise
    finally:
      self.__post_request()

  def logout(self):
    self.__pre_request()
    try:
      res = self._logout()
      return res
    except:
      raise
    finally:
      self.__post_request()

  def new_record(self, rectype, attrs, details):
    attr_keys = ["created",
                 "name", "cluster", "project", "location", "description", "id",
                 "cpus", "memory", "disks", "storage", "primary_ip",
                 "os", "os_family",
                 "status", "is_on",
                 "owner"]
    attrs = {**self.defaults, **attrs}

    attr_json_keys = [ "networks", "storages", "tags"]

    rec = {
      "type": rectype,
      "source": self.name,
      "attributes": None
    }

    for key in attr_keys:
      if not attrs.get(key):
        rec[key] = None
      else:
        rec[key] = attrs[key]
        del(attrs[key])

#    for key in attr_tag_keys:
#      data = attrs.get(key, [])
#      rec[key] = ",".join(map(lambda k: "{}={}".format(k, data[k]), data.keys()))
    
    for key in attr_json_keys:
      if not attrs.get(key):
        rec[key] = '[]'
      else:
        rec[key] = json.dumps(attrs[key], default=str) # added default=str -> problem with AttachTime,CreateTime
        del(attrs[key])

    if "os_family" not in attrs.keys() and rec.get("os"):
      rec["os_family"] = platform.get_os_family(rec.get("os"), rec.get("description"))

    if rec.get("os"):
      rec["os"] = platform.get_os(rec.get("os"), rec.get("description"))

    if len(attrs) > 0:
      rec["attributes"] = json.dumps(attrs, default=str)
    rec["details"] = json.dumps(details, cls=CloudEncoder, default=str)
    return rec

class CloudInvetarioResourceManager:

  def __init__(self, res_list, COLLECTOR_PKG, credentials):
    self.res_list = res_list
    self.COLLECTOR_PKG = COLLECTOR_PKG
    self.credentials = credentials

  def get_resource_objs(self, res_dep_list = None):
    obj_list = {}

    res_list = []
    res_list.extend(res_dep_list or [])
    res_list.extend(self.res_list or [])
    res_list = list(set(res_list))

    for res in res_list:
      try:
        mod_name = self.COLLECTOR_PKG + ".res_collectors." + res
        res_mod = importlib.import_module(mod_name)
      except Exception as e:
        logging.error("Failed to load the following module:{}, reason: {}".format(mod_name, e))
        continue
      obj_list[res] = res_mod.get_resource_obj(self.credentials)

    return obj_list

class CloudInvetarioResource():

  def __init__(self, res_type, credentials):
    self.res_type = res_type
    self.credentials = credentials
    self.client = self.get_client()
    self.data = None

  # def read_data(self):
  #   logging.info("resource collector={}".format(self.res_type))
  #   try:
  #     data = self._read_data()
  #     return data
  #   except Exception as e:
  #     logging.error("An error occured while reading data about following type of cloud resource: {}, reason: {}".format(self.res_type, e))
      
  def fetch(self):
    try:
      return self._fetch()
    except Exception as e:
      print(e)
  
  def get_client(self):
    try:
      return self._get_client()
    except Exception as e:
      logging.error("Failed to get client of following type of cloud resource: {}, reason: {}".format(self.res_type, e))
