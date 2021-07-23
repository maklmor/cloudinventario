import concurrent.futures
import logging
import re
import sys
import asyncio
import time
from pprint import pprint

from google.oauth2 import service_account
import googleapiclient.discovery
from google.cloud import storage

from cloudinventario.helpers import CloudCollector, CloudInvetarioResourceManager

# TEST MODE
TEST = 0

def setup(name, config, defaults, options):
    return CloudCollectorGoogleGCP(name, config, defaults, options)


class CloudCollectorGoogleGCP(CloudCollector):
    def __init__(self, name, config, defaults, options):
        super().__init__(name, config, defaults, options)

    def _config_keys():
        return {
            zone: 'Zone/region of project, Required',
            project_id: 'GCP Project Name from which will be taken data, Required',
            private_key: 'GCP Private Key, Required (Need to replace \n to new lines in yaml)',
            client_email: 'GCP ClientEmail, Required',
            token_uri: 'GCP TokenURI, Required',

            type: 'GCP Type',
            private_key_id: 'GCP PrivateKeyId',
            client_id: 'GCP ClientId',
            auth_uri: 'GCP AuthURI',
            auth_provider_x509_cert_url: 'GCP AuthProviderCertURL',
            client_x509_cert_url: 'GCP ClientCertURL',
        }

    def _login(self):
        credentials = {
            'project_id': self.config['project_id'],
            'token_uri': self.config['token_uri'],
            'client_email': self.config['client_email'],
            'private_key': self.config['private_key'],

            # 'type': self.config['type'],
            # 'private_key_id': self.config['private_key_id'],
            # 'client_id': self.config['client_id'],
            # 'auth_uri': self.config['auth_uri'],
            # 'auth_provider_x509_cert_url': self.config['auth_provider_x509_cert_url'],
            # 'client_x509_cert_url': self.config['client_x509_cert_url']
        }

        self.zone = self.config['zone']
        self.project_name = self.config['project_id']
        self.credentials = service_account.Credentials.from_service_account_info(credentials)
        logging.info("logging in GCP client_email={}, project_name={}".format(self.config['client_email'], self.project_name))

        # GET compute engine
        self.compute_engine = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials, cache_discovery=False)
        return self.compute_engine

    def _fetch(self, collect):
        data = []
        
        # GET all instance with specific project name and zone (return json, where data are in items)
        _instance = self.compute_engine.instances()
        instances = _instance.list(project=self.project_name, zone=self.zone).execute()
        _instance.close()

        for instance in instances['items']:
            if 'name' in instance:
                # GET resources
                resource = _instance.listReferrers(project=self.project_name, zone=self.zone, instance=instance['name']).execute()
                instance['resource'] = resource # Append resources into instance

                # GET machine type
                machine_type_name = re.sub(r".*/machineTypes/", '', instance['machineType']) #'e2-micro'
                _machine_type = self.compute_engine.machineTypes()
                machine_type = _machine_type.get(project=self.project_name, zone=self.zone, machineType=machine_type_name).execute()
                _machine_type.close()
                instance['machineTypeInfo'] = machine_type # Append machine into instance

                # GET subnetworks
                # region_name = re.sub('-[a|b|c]{1}$', '', self.zone)
                # _subnetworks = self.compute_engine.subnetworks()
                # subnetworks = _subnetworks.list(project=self.project_name, region=region_name).execute()
                # _subnetworks.close()
                # instance['subnetworks'] = subnetworks # Append subnetworks into instance

                # GET disks
                _disks = self.compute_engine.disks()
                disks = _disks.list(project=self.project_name, zone=self.zone).execute()
                _disks.close()
                instance['disksInfo'] = disks.get('items') # Append disks into instance

                # Process instance
                data.append(self._process_vm(instance))

        logging.info("Collect {} data".format(len(data)))
        return data

    def _process_vm(self, rec):
        networks = []
        public_ip = rec['networkInterfaces'][0]['accessConfigs'][0].get('natIP') # rec['networkInterfaces'][0].get('accessConfigs')[0].get('natIP') if len(rec['networkInterfaces']) > 0 and 'accessConfigs' in rec['networkInterfaces'][0] and len(rec['networkInterfaces'][0].get('accessConfig')) > 0 else None

        for iface in rec['networkInterfaces']:
            networks.append({
                'name': iface['name'],
                'ip': iface['networkIP'],
                'network': iface['subnetwork'],
                'natIP': iface['accessConfigs'][0].get('natIP') 
                # 'id': iface['NetworkInterfaceId'],
                # 'mac': iface['MacAddress'],
                # 'fqdn': iface['network'],
                # 'connected': (iface['Status'] == 'in-use' and True or False)
            })

        storages = []
        disks_size = 0
        ## Disks Version from request DISK().list() (show all)
        # for disk in rec['disksInfo']:
        #     kind = 'notAttachedDisk'
        #     for attached_disk in rec['disks']:
        #         if attached_disk['deviceName'] == disk['name']:
        #             if attached_disk['kind'] == 'compute#attachedDisk':
        #                 kind = 'attachedDisk'
        #             break
        #     disks_size += (int(disk['sizeGb']) * 1000)
        #     storages.append({
        #         'size': (int(disk['sizeGb']) * 1000),
        #         'name': disk['name'],
        #         'type': re.sub(r'.*/diskTypes/', '', disk['type']),
        #         'created': disk['creationTimestamp'],
        #         'blockSize': disk['physicalBlockSizeBytes'],
        #         'status': disk['status'],
        #         'kind': kind
        #     })
        
        ## Disks Version from request instances.list() (show only attached)
        for disk in rec['disks']:
            type = ''
            created = ''
            blockSize = ''
            status = ''
            for diskInfo in rec['disksInfo']:
                if diskInfo['name'] == disk['deviceName']:
                    type = re.sub(r'.*/diskTypes/', '', diskInfo['type'])
                    created =  diskInfo['creationTimestamp']
                    blockSize =  diskInfo['physicalBlockSizeBytes']
                    status = diskInfo['status']
                    break

            disks_size += (int(disk['diskSizeGb']) * 1000)
            storages.append({
                'size': (int(disk['diskSizeGb']) * 1000),
                'name': disk['deviceName'],
                'kind': disk['kind'],
                'type': type,
                'created': created,
                'blockSize': blockSize,
                'status': status,
            })    
        
        logging.info("new VM name={}".format(rec["name"]))
        vm_data = {
            "created": rec["creationTimestamp"],
            "name": rec["name"],
            "cluster": self.zone,
            "project": self.project_name,
            "description": rec["description"],
            "id": rec["id"],
            "type": rec["machineTypeInfo"].get("name"), 
            "cpus": rec["machineTypeInfo"].get("guestCpus"),
            "memory": rec["machineTypeInfo"].get("memoryMb"),
            "disks": len(rec["disks"]),
            "storage": disks_size, 
            "primary_ip":  rec["networkInterfaces"][0].get("networkIP") if len(rec["networkInterfaces"]) > 0 else None, 
            "primary_fqdn": None, # Not found
            "public_ip": public_ip,
            "public_fqdn": None,  
            "networks": networks,
            "storages": storages,  # Field rec["disks"]
            "monitoring": rec["shieldedInstanceConfig"].get("enableIntegrityMonitoring"),
            "owner": None, # Not found
            "os": None, # Not found
            "status": rec["status"],
            "is_on": (1 if rec["status"] == "RUNNING" else 0),
            "tags": rec["tags"]
        }

        return self.new_record('vm', vm_data, rec)

    def _logout(self):
        self.compute_engine.close()
        self.compute_engine = None
