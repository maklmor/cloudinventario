import boto3, json, logging
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioLightsailDisks(resource, collector)

class CloudInventarioLightsailDisks(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.collector.client
    return client

  def _fetch(self):
    data = []
    paginator = self.client.get_paginator('get_disks')
    response_iterator = paginator.paginate()

    for page in response_iterator:
      for disk in page['disks']:
        data.append(self.process_resource(disk))
    return data

  def _process_resource(self, disk):
    location = disk.get('location', {})
    status = disk.get('state')

    data = {
      "created": disk.get('createdAt'),
      "name": disk.get('name'),
      "cluster": location.get('regionName'),
      "location": location.get('availabilityZone'),
      "storage": disk.get('sizeInGb', 0) * 1024,  # in MB
      "id": disk.get('arn'),
      "is_system_disk": disk.get('isSystemDisk'),
      "path": disk.get('path'),
      "owner": self.collector.account_id,
      "status": status,
      "iops": disk.get('iops'),
      "is_on": True if disk.get('isAttached') and status in ('available', 'in-use') else False,
      "tags": self.collector._get_tags(disk)
    }

    return self.new_record(self.res_type, data, disk)
