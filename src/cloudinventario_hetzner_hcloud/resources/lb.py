from pprint import pprint
import time
from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventariolb(resource, collector)

class CloudInventariolb(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, session):
    return True

  def _get_client(self):
    client = self.session.client('lb')
    return client


  def _fetch(self):
    res = []
    load_balancers = self.collector.client.load_balancers.get_all()
    for lb in load_balancers:
      res.append(self._process_collector(lb))
    return res

  def _process_collector(self,lb):

    data = self.collector._to_dict(lb)
    services = self.collector._to_dict(lb.services)

    instances = []
    #for ins in lb.targets.targets
    print(lb.targets[0].type)
    for ins in data["targets"]:
      if ins.type == "server":
        instances.append({
          "vm": ins.server.id,
        })
      elif ins.type == "ip":
        instances.append({
          "ip": ins.ip.ip,
        })
      elif ins.type == "label_selector":
        # XXX: not working, bad lib
        #for targ in ins.targets:
        #  instances.append({
        #    "vm": targ.server.id,
        #  })
        instances.append({
          "label": ins.label_selector.selector,
        })

    lbdata = {
         "name": data["name"],
         "id": data["id"],
         "created": data["created"],
         "included_traffic": data["included_traffic"],
         "ingoing_traffic": data["ingoing_traffic"],
         #"status": data["targets"]["health_status"]["status"],
         "type": data ["load_balancer_type"]["name"],
         "location": data["location"]["name"],
         #"private_ip": data["private_net"]["ip"],
         "primary_ip": data["public_net"]["ipv4"]["ip"],
       #  "health_status": data["health_status"],
         "instances": instances
    }

    return self.new_record(self.res_type, lbdata, data)
