from pprint import pprint
import logging
import re

from google.oauth2 import service_account
import googleapiclient.discovery

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioGclb(resource, collector)

class CloudInventarioGclb(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, credentials):
    self.credentials = credentials

    self.project_name = self.collector.config['project_id']
    self.client_email = self.collector.config['client_email']
    logging.info("logging config for GCLB client_email={}, project_name={}".format(self.client_email , self.project_name))

  def _fetch(self):
    data = []
    # GET compute engine
    _compute_engine = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials)

    # GET backend services/ info about load balancer
    _backend_services = _compute_engine.backendServices()
    backend_services = _backend_services.list(project=self.project_name).execute()
    _backend_services.close()

    if 'items' in backend_services:

      # GET healthChecks for balancer
      _healthChecks = _compute_engine.healthChecks()
      healthChecks = self._process_health_check(_healthChecks)
      _healthChecks.close()
    
      # GET all address for balancer
      _globalAddreses = _compute_engine.globalAddresses()
      globalAddress = self._process_global_address(_globalAddreses)
      _globalAddreses.close()

      for balancer in backend_services['items']:
        balancer['healthChecks'] = healthChecks
        balancer['globalAddress'] = globalAddress
        # GET instanceGroup and list of managed instances
        balancer['instanceGroups'] = self._process_instances_group(_compute_engine, balancer)

        data.append(self._process_resource(balancer))
    
    logging.info("Collected {} gclb".format(len(data)))
    _compute_engine.close()
    return data
  
  def _process_instances_group(self, _compute_engine, balancer):
    _instanceGroups = _compute_engine.instanceGroups()
    result = []
    for backend in balancer['backends']:
      # Get zone name from group in backends
      zone = re.findall(r'zones/(.*?)/', backend.get('group'))[0]
      # Get instanceGroup name from group in backends
      instanceGroup = re.findall(r'instanceGroups/(.*)', backend.get('group'))[0]

      # Get listInstances
      listInstances = _instanceGroups.listInstances(project=self.project_name, zone=zone, instanceGroup=instanceGroup).execute()
      instances = []
      if 'items' in listInstances:
        for instance in listInstances['items']:
          instances.append({
            'instance': re.findall(r'instances/(.*)', instance.get('instance'))[0],
            'status': instance.get('status')
          })

      # Create result object of backend
      result.append({
        'balancingMode': backend.get('balancingMode'),
        'instanceGroup': instanceGroup,
        'zone': zone,
        'mapInstances': instances
      })

    _instanceGroups.close()
    return result
    
  def _process_health_check(self, _healthChecks):
    healthChecks = _healthChecks.list(project=self.project_name).execute()
    result = []
    if 'items' in healthChecks:
      for healthCheck in healthChecks['items']:
        check = {}
        status = {}
        if 'httpHealthCheck' in healthCheck:
          check['httpHealthCheck'] = healthCheck['httpHealthCheck']
          status['httpHealthCheck'] = healthCheck.get('httpHealthCheck').get('response')
        if 'httpsHealthCheck' in healthCheck:
          check['httpsHealthCheck'] = healthCheck['httpsHealthCheck']
          status['httpsHealthCheck'] = healthCheck.get('httpsHealthCheck').get('response')
        if 'http2HealthCheck' in healthCheck:
          check['http2HealthCheck'] = healthCheck['http2HealthCheck']
          status['http2HealthCheck'] = healthCheck.get('http2HealthCheck').get('response')
        if 'grpcHealthCheck' in healthCheck:
          check['grpcHealthCheck'] = healthCheck['grpcHealthCheck']
          status['grpcHealthCheck'] = healthCheck.get('grpcHealthCheck').get('response')
        if 'sslHealthCheck' in healthCheck:
          check['sslHealthCheck'] = healthCheck['sslHealthCheck']
        if 'tcpHealthCheck' in healthCheck:
          check['tcpHealthCheck'] = healthCheck['tcpHealthCheck']

        result.append({
          'id': healthCheck['id'],
          'name': healthCheck['name'],
          'type': healthCheck['type'],
          'created': healthCheck['creationTimestamp'],
          'healthCheck': check,
          'status': status
        })
    return result

  def _process_global_address(self, _globalAddreses):
    globalAddress = _globalAddreses.list(project=self.project_name).execute()
    result = []
    if 'items' in globalAddress:
      for globalAddress in globalAddress['items']:
        result.append({
          'id': globalAddress.get('id'),
          'name': globalAddress.get('name'),
          'address': globalAddress.get('address'),
          'region': globalAddress.get('region'),
          'status': globalAddress.get('status'),
          'networkTier': globalAddress.get('networkTier'),
          'ipVersion': globalAddress.get('ipVersion'),
          'addressType': globalAddress.get('addressType'),
          'purpose': globalAddress.get('purpose'),
          'subnetwork': globalAddress.get('subnetwork'),
          'network': globalAddress.get('network')
        })
    return result

  def _process_resource(self, balancer):
    # Collect healthCheck status from responses 
    healthCheckStatus = []
    if len(balancer['healthChecks']) > 0:
      for healthCheck in balancer['healthChecks']:
        if healthCheck.get('status'):
          healthCheckStatus.append({
            'status': healthCheck['status']
            })

    logging.info("new gclb name={}".format(balancer.get('name')))
    data = {
      "id": balancer['id'],
      "created": balancer['creationTimestamp'],
      "name": balancer['name'],
      "instances": balancer['healthChecks'],
      "subnets": balancer['globalAddress'], # Not sure
      "scheme": balancer['loadBalancingScheme'],
      "status": healthCheckStatus,
      "backends": balancer['instanceGroups'],
      "project": self.project_name,
      "description": balancer['description']
      # "is_on": True if status == "on" else False, # Not know 
    }

    return self.new_record(self.res_type, data, balancer)
