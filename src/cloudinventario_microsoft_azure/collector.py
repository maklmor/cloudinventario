import concurrent.futures
from copy import Error
import logging, re, sys, asyncio, time
from pprint import pprint
from typing import Dict, List
import datetime

from sqlalchemy.sql.sqltypes import Boolean

from cloudinventario.helpers import CloudCollector

from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient, resources
from azure.mgmt.network import NetworkManagementClient

from azure.mgmt.compute.v2021_03_01.models._models_py3 import VirtualMachine
from azure.mgmt.resource.resources.v2021_04_01.models._models_py3 import (
    GenericResourceExpanded,
)

# TEST MODE
TEST = 0


def setup(name, config, defaults, options):
    return CloudCollectorMicrosoftAzure(name, config, defaults, options)


class CloudCollectorMicrosoftAzure(CloudCollector):
    def __init__(self, name, config, defaults, options):
        super().__init__(name, config, defaults, options)

    def _login(self) -> Boolean:
        """Login into MS Azure Cloud account and get usable client object(s).

        :return: Boolean True value depends on successful login operation
        """
        try:
            subscription_id: str = self.config["subscription_id"]
            self.tenant_id = self.config["tenant_id"]
            client_id = self.config["client_id"]
            client_secret = self.config["client_secret"]

            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )

            self.compute_client = ComputeManagementClient(
                credential=credential, subscription_id=subscription_id
            )
            self.resource_client = ResourceManagementClient(
                credential=credential, subscription_id=subscription_id
            )

            self.network_client = NetworkManagementClient(
                credential=credential, subscription_id=subscription_id
            )

            logging.info("logging in Microsoft Azure Cloud={}".format(self.name))

            return True
        except Error as e:
            logging.error(e)
            return False

    def _fetch(self, collect) -> List[Dict]:
        data: List = []
        list_of_virtual_machines = list(self.compute_client.virtual_machines.list_all())

        resources = list(self.resource_client.resources.list())

        for vm in list_of_virtual_machines:
            data.append(
                self.__process_vm(
                    vm,
                    resources=resources,
                )
            )

        return data

    def __process_vm(
        self,
        vm: VirtualMachine = None,
        resources: List[GenericResourceExpanded] = None,
    ) -> Dict:

        vm_dict: Dict = vm.as_dict()

        created_time: datetime.datetime = self.__get_created_time(resources, vm_dict)
        group_name: str = self.__get_resource_group_name(vm_dict['id'])

        vm_size: str = vm_dict['hardware_profile']['vm_size']
        vm_info: Dict = self.__get_vm_info(vm_dict)

        disks: List = self.__get_disks(group_name, vm_dict)

        os_disk_id = vm_dict.get('storage_profile').get('os_disk').get('managed_disk').get('id')
        os_disk_size = self.__get_os_disk_size(disks, os_disk_id)

        instance_view: Dict = self.__get_vm_instance_view(group_name, vm_dict['name'])

        networks, private_ip_address, public_ip_address = self.__get_networks_info(group_name, vm_dict)
  
        vm_data = {
            "created": created_time,
            "name": vm_dict.get('name'),
            "cluster": None,
            "location": vm_dict.get('location'),
            "project": group_name,
            "description": "",
            "id": vm_dict.get('vm_id'), 
            "type": vm_size,
            "cpus": vm_info.get("number_of_cores"),
            "memory": vm_info.get("memory_in_mb"),
            "disks": len(disks),
            "storage": os_disk_size,
            "primary_ip": private_ip_address or public_ip_address,
            "primary_fqdn": None,
            "public_ip": public_ip_address,
            "public_fqdn": None,
            "networks": networks,
            "storages": disks,
            "monitoring": None,
            "owner": self.tenant_id,
            "os": vm_dict.get('storage_profile').get('os_disk').get('os_type'),
            "os_family": None,
            "status": instance_view.get('statuses')[1].get('display_status'),
            "is_on": (
                instance_view.get('statuses')[1].get('display_status') == "VM running" and 1 or 0
            ),
            "tags": vm_dict.get('tags') or [],
        }

        return self.new_record("vm", vm_data, vm_dict)

    def _logout(self) -> None:
        """Logout from MS Azure Cloud - unable (deallocate) client object(s).

        :return: None
        """
        self.compute_client.close()
        self.compute_client = None

        self.resource_client.close()
        self.resource_client = None
        
        self.network_client.close()
        self.network_client = None

    def __get_created_time(self, resources: List[GenericResourceExpanded] = None, vm_dict: VirtualMachine = None ) -> datetime.datetime:
        """Get and return time of resource creation.

        :param resources: List - all resources included in resource client
        :param vm_dict: Dict - virtual machine data in dictionary form 
        :return: datetime.datetime - time of resource creation
        """
        created_time = [
            resource.created_time
            for resource in resources
            if self.__check_resource_and_vm_id(resource.id, vm_dict.get('id'))
        ][0]

        return created_time

    def __get_vm_info(self, vm_dict: Dict = None) -> Dict:
        """Get and return information about virtual machine.

        :param vm_dict: Dict - virtual machine data in dictionary form 
        :return: Dict - virtual machine info
        """
        vm_sizes_in_location = list(
            self.compute_client.virtual_machine_sizes.list(vm_dict.get('location'))
        )
        vm_info = [
            _vm_size for _vm_size in vm_sizes_in_location if _vm_size.name == vm_dict.get('hardware_profile').get('vm_size')
        ][0]
        return vm_info.as_dict()


    def __get_disks(self, group_name: str = None, vm_dict: Dict = None) -> List[Dict]:
        """Get and return all disks included to (managed by) given virtual machine.

        :param str group_name: group name of the given resource
        :param vm_dict: Dict - virtual machine data in dictionary form 
        :return: List[Dict] - OS and data disk informations in dictionary form
        """
        return [ 
            self.__get_dict_with_details(disk.as_dict(), self.__create_disks_details(vm_dict, disk.as_dict()))
            for disk in self.compute_client.disks.list_by_resource_group(group_name)
        ]

    def __get_dict_with_details(self, _dict: Dict = None, details: List[Dict] = []) -> Dict:
        """Adds a new attribute - details_from_vm - into given data structure

        This is auxiliary methos for list comprehension - it produces a dictionary with a new added attribute.

        :param _dict: Dict - given dicitonary
        :param details: List[Dict] - details to be added
        :return: Dict - modified given dictionary _dict
        """
        _dict['details_from_vm'] = details
        return _dict         

    def __create_disks_details(self, vm_dict: Dict = None, disk_dict: Dict = None) -> List[Dict]:
        """Creates details of virtual machine disks.

        This is auxiliary methos for list comprehension - it produces a List of dictionaries with disks details.
        Details are gathered from virtual machine object data - value of storage_profile attribute.

        :param vm_dict: Dict - given virtual machine
        :param disk_dict: Dict - given disk (OS or data disk)
        :return: List[Dict] - List of dictionaries - details of given disk
        """
        disk_id = disk_dict.get('id')

        disks_specs = list( vm_dict.get('storage_profile').get('data_disks') )
        disks_specs.append( vm_dict.get('storage_profile').get('os_disk') )

        for disk_specs in disks_specs:
            if self.__check_resource_and_vm_id(disk_specs.get('managed_disk').get('id'), disk_id):
                return disk_specs
        
        return None

    def __get_os_disk_size(self, disks: List[Dict] = [], id_to_search: str = None) -> int:
        """Gets and returns size of OS disk (in MB).

        Method grabs size of vm's os disk and converts it to MBs.

        :param disks List[Dict] - disk info
        :param id_to_search str - id of OS disk
        :return: int - size of OS disk in MBs
        """
        for disk in disks:
            if self.__check_resource_and_vm_id(disk.get('id'), id_to_search):
                return disk.get('disk_size_gb') * 1024
        return None

    def __check_resource_and_vm_id(self, resource_id: str, vm_id: str) -> Boolean:
        """Check if the resource attribute id is same as virtual machine id.

        Sometimes is the same group name in different forms - capitalized or not. 
        It's good idea to check it. 

        :param str resource_id: the id attribute of resource obj (disk, network, resource at all)
        :param str vm_id: the id attribute of vm obj
        :return: Boolean - true if ids are same, else False
        """
        resource_id = resource_id.split("/")
        vm_id = vm_id.split("/")

        if len(resource_id) == len(vm_id):
            for resource_element, vm_element in zip(resource_id, vm_id):
                if not resource_element == vm_element:
                    resource_element = resource_element.lower()
                    vm_element = vm_element.lower()
                    if not resource_element == vm_element:
                        return False
        else:
            return False
        return True

    def __get_resource_group_name(self, id: str) -> str:
        """Get resource group name.
        It's kind a tricky thing, cause it's not obvious if it will work in the future.
        Resource group name should be part of every resource id. Sometimes it's capitalized so it's necessary to convert it later.

        :param str vm_id: The id attribute of vm obj
        :return: str - resource group name
        """
        try:
            resource_group_name = id.split("/")[4]
            return resource_group_name
        except Error as e:
            logging.error(e)
            return ""

    def __get_vm_instance_view(self, group_name: str = None, vm_name: str = None) -> Dict:
        """Get vm instance_view attibute value.
        Resource group name follows vm.id attribute but there is one mystery.

        In the resource_client is appropriate disk resource, managed by certain vm
        resource. Disk ID contains part of VM ID but resource group name
        is capitalized. That could throw some errors in future.

        Because of that is group_name rechecked with lower case too.
        
        :param str group_name: group name of the given resource
        :param str vm_name: name of virtual machine
        :return: Dict - instance view of given virtual machine
        """
        return self.compute_client.virtual_machines.instance_view(\
           resource_group_name=group_name, 
           vm_name=vm_name
        ).as_dict()

    def __get_networks_info(self, group_name: str = None, vm_dict: Dict = None) -> tuple:
        """Return networks info - network interfaces, private and public ip addresses
        Private and public ip addresses are gathered via primary network interface.
        Network interface data are mapping according to predetermined structure.

        :param str group_name: group name of the given resource
        :param str vm_dict: vm data in dictionary form
        :return: tuple - network data (List[Dict]), private ip address, public ip address
        """
        networks = []
        private_ip_address = None
        public_ip_address = None
        interfaces = self.network_client.network_interfaces.list(
            resource_group_name=group_name)
        public_ip_addresses = self.network_client.public_ip_addresses.list(
            resource_group_name=group_name)
  
        for net_interface in interfaces:
            net_interface = net_interface.as_dict()

            for vm_net_interface in vm_dict.get('network_profile').get('network_interfaces'):
                if self.__check_resource_and_vm_id(net_interface.get('id'), vm_net_interface.get('id')):
                    networks.append({
                        "id": net_interface.get('id'),
                        "name": net_interface.get('name'),
                        "mac": net_interface.get('mac_address'),
                        "ip": net_interface.get('ip_configurations')[0].get('private_ip_address'),
                        "fqdn": None,
                        "network": net_interface.get('ip_configurations')[0].get('subnet').get('id'),
                        "connected": (net_interface.get('provisioning_state') == "Running" and True or False),
                        "details": net_interface 
                    })
                    
                    if net_interface.get('primary'):
                        for configuration in net_interface.get('ip_configurations'):
                            if configuration.get('primary'):
                                private_ip_address = configuration.get('private_ip_address')
                                public_ip_address_id = configuration.get('public_ip_address').get('id')

                                public_ip_address = [
                                    pub_ip_add.as_dict().get('ip_address')
                                    for pub_ip_add in public_ip_addresses
                                    if pub_ip_add.as_dict().get('id') == public_ip_address_id
                                ][0]
        
        return networks, private_ip_address, public_ip_address
