import sqlite3

class BHLogger:
	def __init__(self):
		self.location = 'log.sqlite';
		self.create_database()
	
	def connect(self):
		self.conn = sqlite3.connect(self.location)
		return self.conn.cursor()
	
	def close(self):
		self.conn.close()
		
	def commit(self):
		self.conn.commit()
	
	def create_database(self):
		curr = self.connect()
		sql = """CREATE TABLE IF NOT EXISTS [action] (
[id] INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
[command] TEXT  NULL,
[entity_type] TEXT  NULL,
[search_params] TEXT NULL,
[new_data] TEXT  NULL,
[created_at] TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL
)"""
		curr.execute(sql)
		sql = """CREATE TABLE IF NOT EXISTS [operation] (
[id] INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
[action_id] INTEGER  NULL,
[entity_id] INTEGER  NULL,
[error] TEXT NULL,
[response] TEXT NULL,
[old_data] TEXT  NULL
)"""
		curr.execute(sql)
		self.close()
		
	def log_message(self, message):
		print message
	
	def log_message_and_quit(self, message):
		self.log_message(message)
		exit()
	
	def log_action(self, command, entityType, search_params, data):
		curr = self.connect()
		sql = 'INSERT INTO [action] (command, entity_type, search_params, new_data) VALUES(?,?,?,?)'
		curr.execute(sql, (command, entityType, search_params, data))
		self.action_id = curr.lastrowid
		self.commit()
		self.close()
	
	def log_operation(self, entity_id, data, response, error):
		if self.action_id:
			curr = self.connect()
			sql = 'INSERT INTO [operation] (action_id, entity_id, error, response, old_data) VALUES(?,?,?,?,?)'
			curr.execute(sql, (self.action_id, entity_id, error, response, data))
			self.commit()
			self.close()
		else:
			raise Exeption("Must log an action before logging an operation")
			
if __name__ == '__main__':
	logger = BHLogger()
	logger.log_action('bulkEdit','Candidate','{firstName:"Angela"}')
	logger.log_operation(1,'{firstname:"Angelah"}')
	
	