import boto3, json, logging
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioLightsailLB(resource, collector)

class CloudInventarioLightsailLB(CloudInvetarioResource):

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
    paginator = self.client.get_paginator('get_load_balancers')
    response_iterator = paginator.paginate()

    for page in response_iterator:
      for lb in page['loadBalancers']:
        data.append(self.process_resource(lb))

    return data

  def _process_resource(self, balancer):
    location = balancer.get('location', {})
    status = balancer.get('state')

    health_states = {}
    for instance in balancer.get('instanceHealthSummary', []):
      health_states[instance.get('instanceName', "unknown_instance")] = {
        "state": instance.get('instanceHealth'),
        "reason": instance.get('instanceHealthReason')
      }

    data = {
      "created": balancer.get('createdAt'),
      "name": balancer.get('name'),
      "cluster": location.get('regionName'),
      "location": location.get('availabilityZone'),
      "id": balancer.get('arn'),
      "instances": health_states,
      "dns_name": balancer.get('dnsName'),
      "owner": self.collector.account_id,
      "status": status,
      "is_on": True if status == "active" or status == "active_impaired" else False,
      "tags": self.collector._get_tags(balancer)
    }

    return self.new_record(self.res_type, data, balancer)
