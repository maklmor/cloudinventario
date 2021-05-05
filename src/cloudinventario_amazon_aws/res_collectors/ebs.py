from ..helpers import CloudInvetarioAWSResource

def get_resource_obj(client):
  return CloudInventarioEbs(client)

class CloudInventarioEbs(CloudInvetarioAWSResource):

  def __init__(self, client):
    super().__init__(client, "ebs storage")

  def _read_data(self):
      storage = {}
      vinfo = self.client.describe_volumes()

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
      return storage
      