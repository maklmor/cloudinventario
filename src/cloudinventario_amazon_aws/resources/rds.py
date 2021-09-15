import boto3, json
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioRds(resource, collector)

class CloudInventarioRds(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.session.client('rds')
    return client

  def _fetch(self):
    data = []
    paginator = self.client.get_paginator('describe_db_instances')
    response_iterator = paginator.paginate()

    for page in response_iterator:
      for db_instance in page['DBInstances']:
        data.append(self.process_resource(db_instance))

    return data

  def _process_resource(self, db):
    storage = db['PendingModifiedValues'].get('AllocatedStorage') or db['AllocatedStorage']
    instance_def = self.collector._get_instance_type(db['DBInstanceClass'][3:])

    data = {
      "name": db.get('DBName'),
      "type": db['Engine'],
      "cpus": instance_def["cpu"],
      "memory": instance_def["memory"],
      "location": db['AvailabilityZone'],
      "created": db['InstanceCreateTime'],
      "status": db['DBInstanceStatus'],
      "address": db['Endpoint']['Address'],
      "maintenance_window": db['PreferredMaintenanceWindow'],
      "encrypted": db['StorageEncrypted'],
      "public": db['PubliclyAccessible'],
      "instance_class": db['PendingModifiedValues'].get('DBInstanceClass') or db['DBInstanceClass'],
      "storage": storage * 1024, # in MiB
      "port": db['PendingModifiedValues'].get('Port') or db['Endpoint']['Port'],
      "multi_az": db['PendingModifiedValues'].get('MultiAZ') or db['MultiAZ'],
      "version": db['PendingModifiedValues'].get('EngineVersion') or db['EngineVersion'],
      "id": db['PendingModifiedValues'].get('DBInstanceIdentifier') or db['DBInstanceIdentifier'],
      "storage_type": db['PendingModifiedValues'].get('StorageType') or db['StorageType'],
      "tags": self.collector._get_tags(db, "TagList")
    }

    return self.new_record(self.res_type, data, db)
