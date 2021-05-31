import boto3, json
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def get_resource_obj(credentials):
  return CloudInventarioElb(credentials)

class CloudInventarioElb(CloudInvetarioResource):

  def __init__(self, credentials):
    super().__init__("elb", credentials)

  def _get_client(self):
    client = boto3.client('elb', aws_access_key_id = self.credentials["access_key"], aws_secret_access_key = self.credentials["secret_key"],
                                  aws_session_token = self.credentials["session_token"], region_name = self.credentials["region"])
    return client

  def _fetch(self):
    data = []
    paginator = self.client.get_paginator('describe_load_balancers')

    for page in paginator.paginate():
      page = self.client.describe_load_balancers(PageSize=100)

      for lb in page['LoadBalancerDescriptions']:
        data.append((self._process_resource(lb), lb))
      
      next_marker = None
      if 'NextMarker' in page:
         next_marker = page['NextMarker']
      if not next_marker:
        break

    return data

  def _process_resource(self, balancer):
    health_info = self.client.describe_instance_health(LoadBalancerName=balancer['LoadBalancerName'])
    health_states = {}
    status = "unknown"

    for instance in health_info['InstanceStates']:
      state = instance['State']
      if state == "InService": # if any in service, service is on
        status = "on"
      elif status == "unknown" and state == "OutOfService":
        status = "off"
      elif status == "unknown" and state == "Unknown":
        status = "unknown"

      health_states[instance['InstanceId']] = {
        "state": instance['State']
      }
    
    data = {
      "created": balancer['CreatedTime'],
      "name": balancer['LoadBalancerName'],
      "cluster": balancer['AvailabilityZones'],
      "id": balancer['CanonicalHostedZoneNameID'],
      "instances": health_states,
      "public_fqdn": balancer['CanonicalHostedZoneName'],
      "owner": self.credentials["account_id"],
      "status": health_states,
      "is_on": True if status == "on" else False,
      "scheme": balancer['Scheme'],
      "subnets": balancer['Subnets'],
      "details": balancer
    }

    for key, value in data.items():
      if type(value) in [dict, list]:
        data[key] = json.dumps(value, default=str)

    return data
