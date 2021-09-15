import logging
import boto3

from cloudinventario_amazon_aws.collector import CloudCollectorAmazonAWS

def setup(name, config, defaults, options):
  return CloudCollectorAmazonLightsail(name, config, defaults, options)

class CloudCollectorAmazonLightsail(CloudCollectorAmazonAWS):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)

  def _get_dependencies(self):
      return []

  def _login(self):
    access_key = self.config['access_key']
    secret_key = self.config['secret_key']
    session_token = self.config.get('session_token')
    self.region = region = self.config['region']
    self.account_id = self.config.get('account_id')

    for logger in ["boto3", "botocore", "urllib3"]:
      logging.getLogger(logger).propagate = False
      logging.getLogger(logger).setLevel(logging.WARNING)

    if self.account_id is None:
      sts = boto3.client('sts', aws_access_key_id = access_key, aws_secret_access_key = secret_key)
      ident = sts.get_caller_identity()
      self.account_id = ident['Account']

    logging.info("logging in AWS account_id={}, region={}".format(self.account_id, region))
    self.session = boto3.Session(aws_access_key_id = access_key, aws_secret_access_key = secret_key,
                                  aws_session_token = session_token, region_name = region)
    self.client = self.session.client('lightsail')

    self.instance_types = {}

    return self.session

  def _fetch(self, collect):
    data = []
    paginator = self.client.get_paginator('get_instances')
    response_iterator = paginator.paginate()

    for page in response_iterator:
      for instance in page['instances']:
        data.append(self._process_vm(instance))

    return data

  def _get_tags(self, data, tag_key="tags"):
    tags = {}
    for tag in data.get(tag_key , []):
      tags[ tag["key"] ] = tag.get("value")
    return tags

  def _process_vm(self, instance):
    hardware = instance.get('hardware', {})
    location = instance.get('location', {})
    state = instance.get('state', {})

    logging.debug("new VM name={}".format(instance.get('name')))

    storage = 0
    storages = []
    for disk in hardware.get('disks', []):
      if disk.get('isAttached'):
        storage += (disk.get('sizeInGb') * 1024)
      storages.append({
        "id": disk.get('name'),
        "cluster": disk.get('location', {}).get('regionName'),
        "storage": disk.get('sizeInGb') * 1024,  # in MB
        "type": disk.get('VolumeType'),
        "status": disk.get('state'),
        "is_on": disk.get('isAttached'),
        "mounts": disk.get('attachedTo'),
        "details": disk
      })

    data = {
      "created": instance.get('createdAt'),
      "name": instance.get('name'),
      "cluster": location.get('regionName'),
      "location": location.get('availabilityZone'),  # cluster or location?
      "id": instance.get('arn'),
      "type": instance.get('bundleId'),
      "cpus": hardware.get('cpuCount'),
      "memory": hardware.get('ramSizeInGb') * 1024, # in MB
      "disks": len(hardware.get('disks')),
      "storage": storage,
      "primary_ip":  instance.get('privateIpAddress') or instance.get('publicIpAddress'),
      "public_ip": instance.get('publicIpAddress'),
      "storages": storages,
      "owner": self.account_id,
      "blueprint_name": instance.get('blueprintName'),  # may contain information about instance os
      "status": state.get('name'),
      "is_on": (state.get('name') == "running" and 1 or 0),
      "tags": self._get_tags(instance)
    }
    return self.new_record('lightsail', data, instance)

  def _logout(self):
      return super()._logout()