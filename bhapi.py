from bhlogger import BHLogger
import config
import urllib
import urllib2
import urlparse
import json
import math
import copy

def json_loads(s):
	try:
		return json.loads(s)
	except ValueError:
		print "Error: Invalid data."
		exit()
	
def json_dumps(obj):
	try:
		return json.dumps(obj)
	except ValueError:
		print "Error: Invalid data."
		exit()

class BHApi:
	base_rest_url = None
	bhrest_token = None
	access_token = None
	refresh_token = None
	
	def __init__(self):
		self.logger = BHLogger()
	
	def get(self,url,jsonEncode=True):
		req = urllib2.Request(url)
		try:
			response = urllib2.urlopen(req)
		except urllib2.HTTPError as e:
			try:
				data = json.loads(e.read())
				if 'errorMessage' in data:
					return data
				else:
					self.logger.log_message_and_quit("HTTP Error (" + str(e.code) + "): " + str(e.reason))
			except ValueError:
				self.logger.log_message_and_quit("HTTP Error (" + str(e.code) + "): " + str(e.reason))
		except urllib2.URLError as e:
			self.logger.log_message_and_quit('Network Error: ' + str(e.reason))
		return json_loads(response.read()) if jsonEncode else response
	
	def post(self,url,params={},jsonEncode=True):
		req = urllib2.Request(url)
		data = ''
		if type(params) is not str:
			data = urllib.urlencode(params)
		else:
			data = params
		try:
			response = urllib2.urlopen(req,data)
		except urllib2.HTTPError as e:
			try:
				data = json.loads(e.read())
				if 'errorMessage' in data:
					return data
				else:
					self.logger.log_message_and_quit("HTTP Error (" + str(e.code) + "): " + str(e.reason))
			except ValueError:
				self.logger.log_message_and_quit("HTTP Error (" + str(e.code) + "): " + str(e.reason))
		except urllib2.URLError as e:
			self.logger.log_message_and_quit('Network Error: ' + str(e.reason))
		return json_loads(response.read()) if jsonEncode else response
	
	def auth(self):
		if self.bhrest_token is None:
			self.logger.log_message("Authenticating...")
			self.logger.log_message("Getting auth code...")
			auth_code = self.get_auth_code()
			self.logger.log_message("Getting access and refresh tokens...")
			access_token = self.set_access_and_refresh_tokens(auth_code)
			self.logger.log_message("Getting BHRest token and base URL...")
			self.set_session_info()
	def refresh(self):
		if self.refresh_token is not None:
			data = self.post(config.AUTH_BASE_URL+'token?grant_type=refresh_token&refresh_token='+self.refresh_token+'&client_id='+config.CLIENT_ID+'&client_secret='+config.CLIENT_SECRET)
			if 'access_token' not in data or 'refresh_token' not in data:
				self.logger.log_message_and_quit("API Error: Unexpected data returned for access/refresh tokens")
			self.access_token, self.refresh_token = data['access_token'],data['refresh_token']
		else:
			raise ValueError("No refresh token available. Call auth() before calling refresh()")
	def get_auth_code(self):
		params = {
			'action': 'Login',
			'username': config.USERNAME,
			'password': config.PASSWORD
		}
		response = self.post(config.AUTH_BASE_URL+'authorize?client_id='+config.CLIENT_ID+'&response_type=code',params, False)
		qs = urlparse.parse_qs(urlparse.urlparse(response.geturl()).query)
		if 'code' not in qs or len(qs['code']) == 0:
			self.logger.log_message_and_quit("API Error: Unexpected data returned for auth code")
		auth_code = urlparse.parse_qs(urlparse.urlparse(response.geturl()).query)['code'][0]
		return auth_code
	
	def set_access_and_refresh_tokens(self, auth_code):
		data = self.post(config.AUTH_BASE_URL+'token?grant_type=authorization_code&code='+auth_code+'&client_id='+config.CLIENT_ID+'&client_secret='+config.CLIENT_SECRET)
		if 'access_token' not in data or 'refresh_token' not in data:
			self.logger.log_message_and_quit("API Error: Unexpected data returned for access/refresh tokens")
		self.access_token, self.refresh_token = data['access_token'],data['refresh_token']
	
	def set_session_info(self):
		data = self.post(config.REST_LOGIN_URL+'?version=*&access_token='+self.access_token)
		if 'BhRestToken' not in data:
			self.logger.log_message_and_quit("API Error: No BHRest token returned")
		self.bhrest_token = data['BhRestToken']
		if 'restUrl' not in data:
			self.logger.log_message_and_quit("API Error: No base URL returned")
		self.base_rest_url = data['restUrl']
	
	def get_api_url(self,command,params = {}):
		params['BhRestToken'] = self.bhrest_token
		qs = urllib.urlencode(params)
		url = self.base_rest_url + command + '?' + qs
		return url
	
	def query(self,entity,where,fields='id'):
		self.auth()
		params = {
			'where': where,
			'fields': fields,
			'count': 50
		}
		url = self.get_api_url('query/'+entity,params)
		print url
		page = self.get(url)
		ret = page
		if 'errorMessage' in ret: return ret
		i = 1
		while page['count'] == params['count']:
			params['start'] = i * params['count']
			url = self.get_api_url('query/'+entity,params)
			page = self.get(url)
			if 'errorMessage' in page: return page
			ret['data'] = ret['data'] + page['data']
			ret['count'] += page['count']
			i += 1
		return ret
	
	def search(self, entity, where,fields='id'):
		self.auth()
		params = {
			'query': where,
			'fields': fields,
			'count': 50
		}
		url = self.get_api_url('search/'+entity,params)
		print url
		ret = self.get(url)
		if 'errorMessage' in ret: return ret
		if ret['total'] > params['count']:
			pageCount = math.ceil(ret['total'] / float(params['count']))
			for i in xrange(1,int(pageCount)):
				params['start'] = i * params['count']
				url = self.get_api_url('search/'+entity,params)
				page = self.get(url)
				if 'errorMessage' in page: return page
				ret['data'] = ret['data'] + page['data']
		return ret 
	
	def get_entity(self,entity, id, fields='*'):
		self.auth()
		url = self.get_api_url('entity/'+entity+'/'+str(id),{'fields':fields})
		response = self.get(url)
		
		return response
		
	def update_entity(self, entity, id, data):
		self.auth()
		url = self.get_api_url('entity/'+entity+'/'+str(id))
		jsdata = json_dumps(data)
		response = self.post(url,jsdata)
		
		return response
		
if __name__=='__main__':
	api = BHApi()
	api.auth()
	data = api.get_entity('Candidate',197,'firstName,lastName')['data']
	print data
	data['firstName'] = 'Angela'
	try:
		print api.update_entity('Candidate',197,data)
	except urllib2.HTTPError as e:
		print e.read()
	print api.get_entity('Candidate',197,'firstName,lastName')
