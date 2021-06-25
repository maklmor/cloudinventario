import boto3, json
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def get_resource_obj(collector):
  return CloudInventarioS3(collector)

class CloudInventarioS3(CloudInvetarioResource):

  def __init__(self, collector):
    super().__init__("s3", collector)

  def _login(self, session):
    self.session = session
    self.client = self.get_client()

  def _get_client(self):
    client = self.session.client('s3')
    return client

  def _fetch(self):
    data = []

    for bucket in self.client.list_buckets()['Buckets']:
      data.append(self._process_resource(bucket['Name']))

    return data

  def _process_resource(self, bucket_name):
    acl = self.client.get_bucket_acl(Bucket=bucket_name)
    location = self.client.get_bucket_location(Bucket=bucket_name)
    ownership_controls = self.client.get_bucket_ownership_controls(Bucket=bucket_name)
    policy_status = self.client.get_bucket_policy_status(Bucket=bucket_name)
    versioning = self.client.get_bucket_versioning(Bucket=bucket_name)
    website = self.client.get_bucket_website(Bucket=bucket_name)
    website.pop('ResponseMetadata')

    data = {
      "acl": acl['Grants'],
      "location": location['LocationConstraint'],
      "ownership_controls": ownership_controls['OwnershipControls'],
      "policy_status": policy_status['PolicyStatus'],
      "versioning": versioning['Status'],
      "website": website,
      "name": bucket_name,
      "id": bucket_name,
      "owner": acl['Owner']['ID']
    }

    return self.collector.new_record(self.res_type, data, data)
