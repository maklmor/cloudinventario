import boto3, json
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource, try_to_return

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

    marker = None
    while True:
      if marker:
        response_iterator = self.client.describe_db_instances(Marker=marker)
      else:
        response_iterator = self.client.describe_db_instances()

      for db_instance in response_iterator['DBInstances']:
        data.append(self.process_resource(db_instance))

      try:
        marker = response_iterator['Marker']
      except Exception:
        break

    return data

  def _process_resource(self, db):

    data = {
      "name": db['DBName'],
      "type": db['Engine'],
      # "cpu": db[''], # TODO cpu number
      # "memory": db[''], # TODO memory size
      "cluster": db['AvailabilityZone'],
      "created": db['InstanceCreateTime'],
      "status": db['DBInstanceStatus'],
      "address": db['Endpoint']['Address'],
      "maintenance_window": db['PreferredMaintenanceWindow'],
      "encrypted": db['Encrypted'],
      "public": db['PubliclyAccessible'],
      "insatnce_class": db.get(
        db['PendingModifiedValues']['DBInstanceClass'],
        db['DBInstanceClass']),
      "storage": db.get(
        db['PendingModifiedValues']['AllocatedStorage'],
        db['AllocatedStorage']) * 1024, # in MiB
      "port": db.get(
        db['PendingModifiedValues']['Port'],
        db['Endpoint']['Port']),
      "multi_az": db.get(
        db['PendingModifiedValues']['MultiAZ'],
        db['MultiAZ']),
      "version": db.get(
        db['PendingModifiedValues']['EngineVersion'],
        db['EngineVersion']),
      "id": db.get(
        db['PendingModifiedValues']['DBInstanceIdentifier'],
        db['DBInstanceIdentifier']),
      "storage_type": db.get(
        db['PendingModifiedValues']['StorageType'],
        db['StorageType'])
    }

    return self.new_record(self.res_type, data, db)
