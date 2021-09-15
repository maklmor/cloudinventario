import boto3

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioEbs(resource, collector)

class CloudInventarioEbs(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.session.client('ec2')
    return client

  def _fetch(self):
    data = []
    pagiantor = self.client.get_paginator('describe_volumes')
    response_iterator = pagiantor.paginate()

    for page in response_iterator:
      for volume in page['Volumes']:
        data.append(self.process_resource(volume))

    return data

  def _process_resource(self, volume):
    mounts = []

    for mnt in volume['Attachments']:
      mounts.append(mnt['InstanceId'])

    data = {
    "id": volume['VolumeId'],
    "cluster": volume['AvailabilityZone'],
    "storage": volume['Size'] * 1024,  # in MB
    "type": volume['VolumeType'],
    "status": volume['State'],
    "is_on": (volume['State'] == 'in-use'),
    "encrypted": volume['Encrypted'],
    "mounts": mounts,
    "details": volume,
    "tags": self.collector._get_tags(volume)
    }

    return self.new_record(self.res_type, data, volume)
