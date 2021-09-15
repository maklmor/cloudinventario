import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

import boto3

from cloudinventario.helpers import CloudCollector, CloudInvetarioResourceManager

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorAmazonAWS(name, config, defaults, options)

class CloudCollectorAmazonAWS(CloudCollector):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)

  def _config_keys():
    return {
       access_key: 'AWS AccessKeyID',
       secret_key: 'AWS SecretAccessKey',
       session_token: 'AWS SessionToken',
       region: 'AWS Region',
       account_id: 'AWS Account'
    }

  def _get_dependencies(self):
    return ["ebs"]

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
    self.client = self.session.client('ec2')

    self.instance_types = {}

    return self.session

  def _fetch(self, collect):
    data = []

    next_token = ""
    while True:
      instances = self.client.describe_instances(MaxResults=100, NextToken=next_token)

      for reservations in instances['Reservations']:
        for instance in reservations['Instances']:
          data.append(self._process_vm(instance))

      next_token = None
      if 'NextToken' in instances:
         next_token = instances['NextToken']
      if not next_token:
        break
    return data

  def _get_instance_type(self, itype):
    if itype not in self.instance_types:
      types = self.client.describe_instance_types(InstanceTypes = [ itype ])
      for rec in types['InstanceTypes']:
        name = rec['InstanceType']
        data = {
          "cpu": rec['VCpuInfo']['DefaultVCpus'],
          "memory": rec['MemoryInfo']['SizeInMiB']
        }
        data['details'] = rec
        self.instance_types[name] = data

    if itype not in self.instance_types:
      raise Exception("Instance type '{}' not found".format(itype))

    return self.instance_types[itype]

  def _get_tags(self, data, tag_key="Tags"):
    tags = {}
    for tag in data.get(tag_key , []):
      tags[ tag["Key"] ] = tag.get("Value")
    return tags

  def _process_vm(self, rec):
    instance_type = rec["InstanceType"]
    instance_def = self._get_instance_type(instance_type)
    tags = self._get_tags(rec)

    networks = []
    for iface in rec["NetworkInterfaces"]:
      networks.append({
        "id": iface["NetworkInterfaceId"],
        "name": iface.get("Name") or iface.get("Description"),
        "mac": iface["MacAddress"],
        "ip": iface["PrivateIpAddress"],
        "fqdn": iface.get("PrivateDnsName"),
        "network": iface["SubnetId"],
        "connected": (iface["Status"] == "in-use" and True or False)
      })
      if iface.get("Association"):
        networks.append({
          "id": iface["NetworkInterfaceId"],
          "type": "virtual",	# like elastic (== shared)
          "name": iface.get("Name") or iface.get("Description"),
          "mac": iface["MacAddress"],
          "ip": iface["Association"].get("PublicIp"),
          "fqdn": iface["Association"].get("PublicDnsName"),
          "connected": True
        })

    # TODO: avoid duplicate counting
    storage = 0
    storages = []
    ebs_data = self.get_resource_data("ebs")
    if ebs_data and rec["InstanceId"] in ebs_data:
      storage = ebs_data[rec["InstanceId"]]["size"]
      storages = ebs_data[rec["InstanceId"]]["storages"]

    name = tags.get("Name") or rec["InstanceId"]
    logging.debug("new VM name={}".format(name))

    ebs = self.resource_collectors["ebs"].get_raw_data()

    storage = 0
    storages = []
    for volume in ebs:
      # XXX: only count storage size on one instance
      if volume["mounts"] and rec["InstanceId"] == volume["mounts"][0]:
        storage += volume["storage"]
        storages.append(volume)

    vm_data = {
        "created": None,
        "name": name,
        "cluster": rec["Placement"]["AvailabilityZone"],
        "project": rec["Placement"]["GroupName"],
        "description": None,
        "id": rec["InstanceId"],
        "type": instance_type,
        "cpus": rec["CpuOptions"]["CoreCount"] or instance_def["cpu"],
        "memory": instance_def["memory"],
        "disks": len(ebs),
        "storage": storage,
        "primary_ip":  rec.get("PrivateIpAddress") or rec.get("PublicIpAddress"),
        "primary_fqdn": rec.get("PrivateDnsName") or rec.get("PublicDnsName"),
        "public_ip": rec.get("PublicIpAddress"),
        "public_fqdn": rec.get("PublicDnsName"),
        "networks": networks,
        "storages": storages,
        #"storage_ebs_optimized": rec.get("EbsOptimized") or False,
        "monitoring": rec.get("Monitoring"),
        "owner": self.account_id,
        "os": rec.get("Platform"),
        "status": rec["State"]["Name"],
        "is_on": (rec["State"]["Name"] == "running" and 1 or 0),
        "tags": tags
    }

    return self.new_record('vm', vm_data, rec)

  def _logout(self):
    self.client = None


