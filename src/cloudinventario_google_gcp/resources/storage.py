import json
from pprint import pprint
import logging

from google.cloud import storage
from google.cloud.storage import bucket
from google.oauth2 import service_account
import googleapiclient.discovery

from cloudinventario.helpers import CloudInvetarioResource


def setup(resource, collector):
    return CloudInventarioStorage(resource, collector)


class CloudInventarioStorage(CloudInvetarioResource):

    def __init__(self, resource, collector):
        super().__init__(resource, collector)

    def _login(self, credentials):
        self.credentials = credentials

        self.project_name = self.collector.config['project_id']
        self.client_email = self.collector.config['client_email']
        logging.info("logging config for GCP storage client_email={}, project_name={}".format(self.client_email , self.project_name))

    def _fetch(self):
        data = []
        # GET storages
        self.storage = googleapiclient.discovery.build('storage', 'v1', credentials=self.credentials)

        # GET all buckets in specific project
        _buckets = self.storage.buckets()
        buckets = _buckets.list(project=self.project_name).execute()
        _buckets.close()

        # GET owner email
        _serviceAccount = self.storage.projects().serviceAccount()
        service_account = _serviceAccount.get(projectId=self.project_name).execute().get('email_address')
        _serviceAccount.close()

        for bucket in buckets['items']:
            bucket['email_address'] = service_account

            # # GET acl
            # _bucketAccessControls = self.storage.bucketAccessControls()
            # accessControls = _bucketAccessControls.get(bucket=bucket['name'], entity=('user-' + self.client_email)).execute()
            # bucket['acl'] = accessControls
            # _bucketAccessControls.close()

            # # GET owner acl
            # _defaultObjectAccessControls = self.storage.defaultObjectAccessControls()
            # ownerAccessControl = _defaultObjectAccessControls.get(bucket=bucket['name'], entity='user-' + service_account.get('email_address')).execute()
            # _defaultObjectAccessControls.close()
            # bucket['ownerACL'] = ownerAccessControl

            data.append(self._process_resource(bucket))

        logging.info("Collected {} storages".format(len(data)))
        self.storage.close()
        return data

    def _process_resource(self, bucket):
        logging.info("new storage name={}".format(bucket.get('name')))
        data = {
            "acl": bucket['acl'] if 'acl' in bucket else None,
            "location": bucket['location'],
            "ownership_controls": bucket['ownerACL'] if 'ownerACL' in bucket else None,
            "policy_status": bucket['iamConfiguration'],
            "versioning": bucket['versioning'].get('enabled') if 'versioning' in bucket else False,
            "website": bucket['website'] if 'website' in bucket else bucket['selfLink'],
            "name": bucket['name'],
            "id": bucket['id'],
            "created": bucket['timeCreated'],
            "project": self.project_name
        }

        return self.new_record(self.res_type, data, bucket)

    def _logout(self):
        self.credentials = None
