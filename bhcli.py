from bhapi import BHApi
from bhlogger import BHLogger
import json
import getopt
import sys
import re

class BHCLI():
	def __init__(self,action,args):
		opts, args = getopt.getopt(args,'',['entity=','query=','search=','set=', 'test'])
		self.action, self.opts, self.args = action, opts, args
		self.logger = BHLogger()
		self.api = BHApi()
	
	def run(self):
		self.dispatch()
	
	def dispatch(self):
		if self.action == 'bulkEdit':
			self.bulkEdit()
		elif self.action == 'search':
			self.search()
		else:
			self.logger.log_message_and_quit("Unrecognized action: " + self.action)
	
	def search(self):
		opts = {k:v for k,v in self.opts}
		if '--entity' not in opts:
			self.logger.log_message_and_quit("No entity type specified. Use --entity")
		if '--search' not in opts:
			self.logger.log_message_and_quit("No where clause specified. Please use --search")
		response = self.api.search(opts['--entity'],opts['--search'],'id')
		if 'data' in response:
			self.logger.log_message(response['data'])
		elif 'errorMessage' in response:
			self.logger.log_message_and_quit(response['errorMessage'])
		
	def bulkEdit(self):
		response = {}
		new_data = {}
		fields = []
		data = []
		where = ''
		opts = {k:v for k,v in self.opts}
		if '--entity' not in opts:
			self.logger.log_message_and_quit("No entity type specified. Use --entity")
		if '--set' not in opts:
			self.logger.log_message_and_quit("No set clause specified. Use --set")
		elif not re.match('^(\s*\w+\s*=((\s*".*")|([^\,]+))\s*\,)*(\s*\w+\s*=((\s*".*")|([^\,]+)))$',opts['--set']):
			self.logger.log_message_and_quit("Invalid set clause")
		else:
			for match in re.finditer('\s*(\w+)\s*=\s*(?:(".*")|([^\,]+))\s*(?=\,|$)', opts['--set']):
				groups = match.groups()
				fields.append(groups[0])
				new_data[groups[0]] = groups[2].strip() if groups[2] else groups[1][1:-1]
		if '--query' in opts:
			self.logger.log_message("Querying for target entities...")
			response =  self.api.query(opts['--entity'],opts['--query'],','.join(fields + ['id',]))
			where = opts['--query']
		elif '--search' in opts:
			self.logger.log_message("Searching for target entities...")
			response = self.api.search(opts['--entity'],opts['--search'],','.join(fields + ['id',]))
			where = opts['--search']
		else:
			self.logger.log_message_and_quit("No where clause specified. Please use --search or --query")
		if 'data' in response:
			if '--test' not in opts: self.logger.log_message("Editing " + (str(response['total']) if 'total' in response else str(response['count'])) + " target entities...")
			else: self.logger.log_message("Testing " + (str(response['total']) if 'total' in response else str(response['count'])) + " target entities...")
			action = 'bulkEdit' if '--test' not in opts else 'test:bulkEdit'
			self.logger.log_action(action,opts['--entity'],where,json.dumps(new_data))
			for d in response['data']:
				error = ''
				r = ''
				if '--test' not in opts:
					r = self.api.update_entity(opts['--entity'],d['id'],new_data)
					error = r['errorMessage'] if 'errorMessage' in r else ""
					if error: self.logger.log_message("Logging API Error")
				self.logger.log_operation(d['id'], json.dumps({k:d[k] for k in d if k != '_score'}), str(r), error)
		elif 'errorMessage' in response:
			self.logger.log_message_and_quit("API Error: " + response['errorMessage'])
		
		if '--test' not in opts: self.logger.log_message("Done.")
		else: self.logger.log_message("Test Complete.")
		
cli = BHCLI(sys.argv[1], sys.argv[2:])
cli.run()
