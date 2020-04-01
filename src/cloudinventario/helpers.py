"""Classes used by CloudInventario."""
import requests
import json

import cloudinventario.platform as platform

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
                 "owner", "tags"]
    attrs = {**self.defaults, **attrs}

    attr_json_keys = [ "networks", "storages" ]
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

    for key in attr_json_keys:
      if not attrs.get(key):
        rec[key] = '[]'
      else:
        rec[key] = json.dumps(attrs[key])
        del(attrs[key])

    if "os_family" not in attrs.keys() and rec.get("os"):
      rec["os_family"] = platform.get_os_family(rec.get("os"), rec.get("description"))

    if len(attrs) > 0:
      rec["attributes"] = json.dumps(attrs)
    rec["details"] = json.dumps(details)
    return rec
