from pprint import pprint
import logging
import re

from google.oauth2 import service_account
import googleapiclient.discovery

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioCloudSQL(resource, collector)

class CloudInventarioCloudSQL(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

  def _login(self, credentials):
    self.credentials = credentials

    self.project_name = self.collector.config['project_id']
    self.client_email = self.collector.config['client_email']
    logging.info("logging config for cloudSQL client_email={}, project_name={}".format(self.client_email , self.project_name))

  def _fetch(self):
    data = []
    # GET sqladmin
    _sqladmin = googleapiclient.discovery.build('sqladmin', 'v1beta4', credentials=self.credentials)

    # GET instances
    _instances = _sqladmin.instances()
    instances = _instances.list(project=self.project_name).execute()
    _instances.close()

    # GET tiers (Lists all available machine types for Cloud SQL)
    _tiers = _sqladmin.tiers()
    tiers = _tiers.list(project=self.project_name).execute()
    _tiers.close()

    for instance in instances.get('items', []):
      # find tier in instance
      instance_tier = instance.get('settings').get('tier') if 'settings' in instance else None
      instance['tierDetail'] = next((tier for tier in tiers.get('items', []) if str(tier.get('tier')) == instance_tier), None)
            
      data.append(self._process_resource(instance))
    
    logging.info("Collected {} cloudSQL".format(len(data)))
    _sqladmin.close()
    return data

  def _process_resource(self, instance):
    mb = float(1<<20) # Megabytes
    # mb = float(1<<17) # Megabits

    memory = (int(instance['tierDetail'].get('RAM')) / mb) if 'tierDetail' in instance and 'RAM' in instance['tierDetail'] else 0
    storage = (int(instance.get('settings').get('dataDiskSizeGb')) * 1024) if 'settings' in instance else 0
    disks = (int(instance['tierDetail'].get('DiskQuota')) / mb) if 'tierDetail' in instance and 'DiskQuota' in instance['tierDetail'] else 0
    primary_ip = next((ip.get('ipAddress') for ip in instance.get('ipAddresses') if ip.get('type') == 'PRIMARY'), None)

    logging.info("new cloudSQL name={}".format(instance.get('name')))
    data = {
        'dbVersion': re.sub(r'.*_', '', instance.get('databaseVersion')),
        'dbType': re.sub(r'_.*', '', instance.get('databaseVersion')),
        'name': instance.get('name'),
        'id': instance.get('name'),
        'cluster': instance.get('gceZone'),
        'project': instance.get('project'),
        'location': instance.get('region'),
        'created': instance.get('serverCaCert').get('createTime') if 'serverCaCert' in instance else None,
        'memory': round(memory), # Max RAM size 
        'disks': disks, # Max Disk size
        'storage': storage, # Data disk size
        # 'cpu':, NOT PROVIDED
        'diskType': instance.get('settings').get('dataDiskType') if 'settings' in instance else None,
        'status': instance.get('state'),
        'primary_ip': primary_ip,
        'networks': instance.get('ipAddresses'),
        'tags': instance.get('settings').get('userLabels'),
        'is_on': 1 if instance.get('state') == "RUNNABLE" else 0,

        'instanceType': instance.get('instanceType'),
        'backendType': instance.get('backendType'),
    }

    return self.new_record(self.res_type, data, instance)
