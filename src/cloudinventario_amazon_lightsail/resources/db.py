import boto3, json, logging
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioLightsailDB(resource, collector)

class CloudInventarioLightsailDB(CloudInvetarioResource):

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
    paginator = self.client.get_paginator('get_relational_databases')
    response_iterator = paginator.paginate()

    for page in response_iterator:
      for db in page['relationalDatabases']:
        data.append(self.process_resource(db))
    return data

  def _process_resource(self, db):
    location = db.get('location', {})
    hardware = db.get('hardware', {})
    status = db.get('state')
    endpoint = db.get('masterEndpoint', {})

    data = {
      "name": db.get('name'),
      "type": db.get('engine'),
      "cpus": hardware.get('cpuCount'),
      "memory": hardware.get('ramSizeInGb', 0) * 1024,  # in MB
      "created": db.get('createdAt'),
      "cluster": location.get('regionName'),
      "location": location.get('availabilityZone'),
      "storage": hardware.get('diskSizeInGb', 0) * 1024,  # in MB
      "id": db.get('arn'),
      "version": db.get('pendingModifiedValues', {}).get('EngineVersion') or db.get('engineVersion'),
      "owner": self.collector.account_id,
      "status": status,
      "port": endpoint.get('port'),
      "address": endpoint.get('address'),
      "public": db.get('publiclyAccessible'),
      "tags": self.collector._get_tags(db)
    }

    return self.new_record(self.res_type, data, db)
