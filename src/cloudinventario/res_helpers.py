import importlib, logging

class CloudInvetarioResourceManager:

	def __init__(self, res_list, client):
		self.res_list = res_list
		self.client = client

	def get_resource_data(self):
		data = {}
		
		for res in self.res_list:
			res_mod = importlib.import_module("cloudinventario_amazon_aws.res_collectors." + res)
			res_obj = res_mod.get_resource_obj(self.client)
			data[res] = res_obj.read_data()

		return data

class CloudInvetarioAWSResource():

	def __init__(self, client, res_type):
		self.client = client
		self.res_type = res_type

	def read_data(self):
		try:
			data = self._read_data()
			return data
		except Exception:
			logging.error("An error occured while reading data about following type of AWS resource: {}", self.res_type)
			