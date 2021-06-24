import boto3

from cloudinventario.helpers import CloudInvetarioResource

def get_resource_obj(collector):
  return CloudInventarioEbs(collector)

class CloudInventarioEbs(CloudInvetarioResource):

  def __init__(self, collector):
    super().__init__("ebs", collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.session.client('ec2')
    return client

  def _fetch(self):
    storage = {}

    next_token = ""
    while True:
      vinfo = self.client.describe_volumes(MaxResults=100, NextToken=next_token)

      for volume in vinfo['Volumes']:
        # XXX: sorting attachments for stable summing
        attachments = sorted(volume['Attachments'], key=lambda k: k['InstanceId'])
        for idx in range(0, len(attachments)):
          atch = volume['Attachments'][idx]
          instance_id = atch['InstanceId']
          if instance_id not in storage:
            storage[instance_id] = {
              "size": 0,
              "storages": []
            }

          # XXX: only count storage size on one instance
          if idx == 0:
            storage[instance_id]["size"] += volume['Size'] * 1024

          storage[instance_id]["storages"].append({
          "id": volume['VolumeId'],
          "name": atch['Device'],
          "capacity": volume['Size'] * 1024,  # in MB
          "free": None,
          "type": volume['VolumeType'],
          "encrypted": volume['Encrypted'],
          "details": volume
        })

      next_token = None
      if 'NextToken' in vinfo:
         next_token = vinfo['NextToken']
      if not next_token:
        break

    return storage
