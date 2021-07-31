import logging
import boto3, json
from pprint import pprint

from cloudinventario.helpers import CloudInvetarioResource

def setup(resource, collector):
  return CloudInventarioS3(resource, collector)

class CloudInventarioS3(CloudInvetarioResource):

  def __init__(self, resource, collector):
    super().__init__(resource, collector)

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
    details = {}

    try: # acl
      acl = self.client.get_bucket_acl(Bucket=bucket_name)
      acl.pop("ResponseMetadata", None)
      details["acl"] = acl
      acl = acl['Grants']
    except Exception:
      acl = None
      logging.info("The acl of the following bucket was not found: {}, you need the \"READ_ACP\" permission".format(bucket_name))

    try: # location
      location = self.client.get_bucket_location(Bucket=bucket_name)
      location.pop("ResponseMetadata", None)
      details["location"] = location['LocationConstraint']
      location = location
    except Exception:
      location = None
      logging.info("The acl of the following bucket was not found: {}, you must be owner".format(bucket_name))

    try: # ownership controls
      ownership_controls = self.client.get_bucket_ownership_controls(Bucket=bucket_name)['OwnershipControls']
      ownership_controls.pop("ResponseMetadata", None)
      details["ownership_controls"] = ownership_controls
    except Exception:
      ownership_controls = None
      logging.info("The ownership controls of the following bucket were not found: {}, you need the \"S3:GetBucketOwnershipControls\" permission".format(bucket_name))

    try: # policy status
      policy_status = self.client.get_bucket_policy_status(Bucket=bucket_name)['PolicyStatus']
      policy_status.pop("ResponseMetadata", None)
      details["policy_status"] = policy_status
    except Exception:
      policy_status = None
      logging.info("The acl of the following bucket was not found: {}, you need the \"S3:GetBucketPolicyStatus\" permission".format(bucket_name))

    try: # website
      website = self.client.get_bucket_website(Bucket=bucket_name)
      website.pop("ResponseMetadata", None)
      details["website"] = website
    except Exception:
      website = None
      logging.info("The website of the following bucket was not found: {}, you need the \"S3:GetBucketWebsite\" permission".format(bucket_name))

    try: # versioning
      versioning = self.client.get_bucket_versioning(Bucket=bucket_name)
      versioning.pop("ResponseMetadata", None)
      details["versioning"] = versioning
      versioning = versioning['Status']
    except Exception:
      versioning = None
      logging.info("The acl of the following bucket was not found: {}, you must be owner".format(bucket_name))

    data = {
      "acl": acl,
      "location": location,
      "ownership_controls": ownership_controls,
      "policy_status": policy_status,
      "versioning": versioning,
      "website": website,
      "name": bucket_name,
      "id": bucket_name,
      "owner": acl['Owner']['ID']
    }

    return self.new_record(self.res_type, data, details)
