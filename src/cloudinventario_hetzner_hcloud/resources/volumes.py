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
    client = self.session.client('vinfo')
    return client


  def _fetch(self):
    data = []
    vinfo = self.collector.client.volumes.get_all()
    
    for volume in vinfo:
      data.append(self._process_collector(volume))
    return data

  def _process_collector(self,volume):
    volume = self.collector._to_dict(volume)



    vinfo = {
     "id": volume["id"],
     "name": volume["name"],
     "location": volume['location']["name"],
     "storage": volume['size'] * 1024,  # in MB
     "type": volume['linux_device'],
     "is_on": (volume['status'] == "available"),

     }

    return self.new_record(self.res_type, vinfo, volume) 