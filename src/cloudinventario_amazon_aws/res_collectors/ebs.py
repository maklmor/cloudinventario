import boto3

from cloudinventario.helpers import CloudInvetarioResource

def get_resource_obj(credentials):
  return CloudInventarioEbs(credentials)

class CloudInventarioEbs(CloudInvetarioResource):

  def __init__(self, credentials):
    super().__init__("ebs", credentials)

  def _get_client(self):
    client = boto3.client('ec2', aws_access_key_id = self.credentials[0], aws_secret_access_key = self.credentials[1],
                                  aws_session_token = self.credentials[2], region_name = self.credentials[3])
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
    

  # def _read_data(self):
  #     storage = {}
  #     vinfo = self.client.describe_volumes()

  #     for volume in vinfo['Volumes']:
  #       # XXX: sorting attachments for stable summing
  #       attachments = sorted(volume['Attachments'], key=lambda k: k['InstanceId']) 
  #       for idx in range(0, len(attachments)):
  #         atch = volume['Attachments'][idx]
  #         instance_id = atch['InstanceId']
  #         if instance_id not in storage:
  #           storage[instance_id] = {
  #             "size": 0,
  #             "storages": []
  #           }

  #         # XXX: only count storage size on one instance
  #         if idx == 0:
  #           storage[instance_id]["size"] += volume['Size'] * 1024

  #         storage[instance_id]["storages"].append({
  #         "id": volume['VolumeId'],
  #         "name": atch['Device'],
  #         "capacity": volume['Size'] * 1024,  # in MB
  #         "free": None,
  #         "type": volume['VolumeType'],
  #         "encrypted": volume['Encrypted'],
  #         "details": volume
  #       })
  #     return storage
      