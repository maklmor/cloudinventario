import concurrent.futures
import logging, re, sys, asyncio, time
from pprint import pprint

import boto3

from cloudinventario.helpers import CloudCollector

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
  return CloudCollectorAmazonAWS(name, config, defaults, options)

class CloudCollectorAmazonAWS(CloudCollector):

  def __init__(self, name, config, defaults, options):
    super().__init__(name, config, defaults, options)


  def _login(self):
    access_key = self.config['access_key']
    secret_key = self.config['secret_key']
    region = self.config['region']

    for logger in ["boto3", "botocore", "urllib3"]:
      logging.getLogger(logger).propagate = False
      logging.getLogger(logger).setLevel(logging.WARNING)

    # TODO: if region == ALL, loop all regions slowly or parallely ?

    logging.info("logging in AWS region={}".format(region))
    self.client = boto3.client('ec2', aws_access_key_id = access_key, aws_secret_access_key = secret_key, region_name = region)
    self.instance_types = {}
    return True

  def _fetch(self, collect):
    res = []
    self.storage = self._get_storage_info() 

    next_token = ""
    while True:
      instances = self.client.describe_instances(MaxResults=100, NextToken=next_token)

      for reservations in instances['Reservations']:
        for instance in reservations['Instances']:
          res.append(self._process_vm(instance))

      next_token = None
      if 'NextToken' in instances:
         next_token = instances['NextToken']
      if not next_token:
        break

    return res

  def _get_instance_type(self, itype):
    if itype not in self.instance_types:
      types = self.client.describe_instance_types(InstanceTypes = [ itype ])
      for rec in types['InstanceTypes']:
        name = rec['InstanceType']
        data = {
          "cpu": rec['VCpuInfo']['DefaultVCpus'],
          "memory": rec['MemoryInfo']['SizeInMiB']
        }
        # if rec.get("InstanceStorageInfo"):
        #   data["storage"] = rec['InstanceStorageInfo']['TotalSizeInGB'] * 1024
        #   for disk in rec['InstanceStorageInfo']['Disks']:
        #     data['storages'].append({
        #       "id": None,
        #       "name": None,
        #       "capacity": disk['SizeInGB'] * 1024,  # in MB
        #       "free": None,
        #       "type": disk['Type'],
        #       "ssd": (disk['Type'] == 'ssd') or 0
        #     })
        data['details'] = rec
        self.instance_types[name] = data

    if itype not in self.instance_types:
      raise Exception("Instance type '{}' not found".format(itype))

    return self.instance_types[itype]

  def _get_storage_info(self):
    storage = {}

    vinfo = self.client.describe_volumes()

    for volume in vinfo['Volumes']:
      for atch in volume['Attachments']:

        if atch['InstanceId'] not in storage:
          storage[atch['InstanceId']] = {
            "size": 0,
            "storages": []
          }
        storage[atch['InstanceId']]["size"] += volume['Size'] * 1024
        storage[atch['InstanceId']]["storages"].append({
        "id": volume['VolumeId'],
        "name": atch['Device'],
        "capacity": volume['Size'] * 1024,  # in MB
        "free": None,
        "type": None,
        "ssd": None,
        "details": volume
      })

    return storage

  def _process_vm(self, rec):
    instance_type = rec["InstanceType"]
    instance_def = self._get_instance_type(instance_type)

    networks = []
    for iface in rec["NetworkInterfaces"]:
      networks.append({
        "id": iface["NetworkInterfaceId"],
        "name": iface.get("Name") or iface.get("Description"),
        "mac": iface["MacAddress"],
        "ip": iface["PrivateIpAddress"],
        "fqdn": iface["PrivateDnsName"],
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

    tags = {}
    for tag in rec.get("Tags", []):
      tags[ tag["Key"] ] = tag["Value"]

    name = tags.get("Name") or rec["InstanceId"]
    logging.debug("new VM name={}".format(name))
    return self.new_record('vm', {
      "created": None,
      "name": name,
      "cluster": rec["Placement"]["AvailabilityZone"],
      "project": rec["Placement"]["GroupName"],
      "description": None,
      "id": rec["InstanceId"],
      "type": instance_type,
      "cpus": rec["CpuOptions"]["CoreCount"] or instance_def["cpu"],
      "memory": instance_def["memory"],
      "disks": None,	# TODO
      "storage": self.storage[rec["InstanceId"]]["size"],
      "primary_ip":  rec.get("PrivateIpAddress") or rec.get("PublicIpAddress"),
      "primary_fqdn": rec.get("PrivateDnsName") or rec.get("PublicDnsName"),
      "public_ip": rec.get("PublicIpAddress"),
      "public_fqdn": rec.get("PublicDnsName"),
      "networks": networks,
      "storages": self.storage[rec["InstanceId"]]["storages"],
      #"storage_ebs_optimized": rec.get("EbsOptimized") or False,
      "monitoring": rec.get("Monitoring"),
#      "owner": None,
      "os": rec.get("Platform"),
      "status": rec["State"]["Name"],
      "is_on": (rec["State"]["Name"] == "running" and 1 or 0),
      "tags": tags,
    }, rec)

  def _logout(self):
    self.client = None