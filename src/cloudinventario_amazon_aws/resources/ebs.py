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

    next_token = ""
    while True:
      vinfo = self.client.describe_volumes(MaxResults=100, NextToken=next_token)

      for volume in vinfo['Volumes']:
        data.append(self.process_resource(volume))

      next_token = None
      if 'NextToken' in vinfo:
         next_token = vinfo['NextToken']
      if not next_token:
        break

    return data

  def _process_resource(self, volume):
    mounts = []
    
    for mnt in volume['Attachments']:
      mounts.append(mnt['InstanceId'])

    data = {
    "id": volume['VolumeId'],
    "cluster": volume['AvailabilityZone'],
    "capacity": volume['Size'] * 1024,  # in MB
    "type": volume['VolumeType'],
    "encrypted": volume['Encrypted'],
    "mounts": mounts,
    "details": volume
    }

    return self.new_record(self.res_type, data, volume)
