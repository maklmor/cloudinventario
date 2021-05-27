import boto3, json
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def get_resource_obj(credentials):
  return CloudInventarioElb(credentials)

class CloudInventarioElb(CloudInvetarioResource):

  def __init__(self, credentials):
    super().__init__("elb", credentials)

  # def _read_data(self):
  #   elb_info = self.client.describe_load_balancers()
  #   data = {} # instance_id: load_balancers

  #   for balancer in elb_info['LoadBalancerDescriptions']:
  #     ec2_instances  = list( map(lambda x: x['InstanceId'], balancer['Instances']))

  #     for instance in ec2_instances:
  #       health_info = self.client.describe_instance_health(LoadBalancerName= balancer['LoadBalancerName'], Instances= [{'InstanceId': instance}])
  #       port_list = dict( map(lambda x: (x['Listener']['LoadBalancerPort'], x['Listener']['Protocol']), balancer['ListenerDescriptions']))
  #       if not data.get(instance):
  #         data[instance] = [{
  #           "name": balancer['LoadBalancerName'],
  #           "listener ports": port_list,
  #           "scheme": balancer['Scheme'],
  #           "health of connection": health_info['InstanceStates'][0]['State'],
  #           "health of connection reason": health_info['InstanceStates'][0]['ReasonCode'],
  #           "details": balancer
  #         }]
  #   return data

  def _get_client(self):
    client = boto3.client('elb', aws_access_key_id = self.credentials[0], aws_secret_access_key = self.credentials[1],
                                  aws_session_token = self.credentials[2], region_name = self.credentials[3])
    return client

  def _fetch(self):
    data = []

    next_marker = ""
    while True:
      load_balancers = self.client.describe_load_balancers(PageSize=100)

      for lb in load_balancers['LoadBalancerDescriptions']:
        data.append((self._process_resource(lb), lb))
      
      next_marker = None
      if 'NextMarker' in load_balancers:
         next_marker = load_balancers['NextMarker']
      if not next_marker:
        break

    return data

  def _process_resource(self, balancer):
    health_info = self.client.describe_instance_health(LoadBalancerName=balancer['LoadBalancerName'])
    health_states = {}
    for instance in health_info['InstanceStates']:
      health_states[instance['InstanceId']] = {
        "state": instance['State'],
        "reason": instance['ReasonCode']
      }

    data = {
      "created": balancer['CreatedTime'],
      "name": balancer['LoadBalancerName'],
      "cluster": balancer['AvailabilityZones'],
      "id": balancer['CanonicalHostedZoneNameID'],
      "public_fqdn": balancer['CanonicalHostedZoneName'],
      "owner": self.credentials[4],
      "status": health_states,
      "scheme": balancer['Scheme'],
      "subnets": balancer['Subnets']
    }

    for key, value in data.items():
      if type(value) in [dict, list]:
        data[key] = json.dumps(value, default=str)

    return data
