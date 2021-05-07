"""Classes used by CloudInventario."""
import requests
import datetime
import json
import logging
import importlib
#from pprint import pprint

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

    attr_json_keys = [ "networks", "storages", "tags" ]
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
      rec["attributes"] = json.dumps(attrs)
    rec["details"] = json.dumps(details, cls=CloudEncoder)
    return rec

class CloudInvetarioResourceManager:

	def __init__(self, res_list, client, cloud_col):
		self.res_list = res_list
		self.client = client
		self.cloud_col = cloud_col

	def get_resource_data(self, res_dep_list = None):
		data = {}

		res_list = []
		res_list.extend(res_dep_list or [])
		res_list.extend(self.res_list or [])
		res_list = list(set(res_list))

		for res in res_list:
			res_mod = importlib.import_module(self.cloud_col + ".res_collectors." + res)
			res_obj = res_mod.get_resource_obj(self.client)
			data[res] = res_obj.read_data()

		return data

class CloudInvetarioResource():

	def __init__(self, client, res_type):
		self.client = client
		self.res_type = res_type

	def read_data(self):
		try:
			data = self._read_data()
			return data
		except Exception:
			logging.error("An error occured while reading data about following type of cloud resource: {}", self.res_type)
