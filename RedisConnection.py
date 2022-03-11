import redis


class RedisConnection:
	db_connection: redis.Redis

	def __init__(self, host: str, port: int, passphrase: str, user: str):
		self.db_connection = redis.Redis(host=host, port=port, password=passphrase, username=user)

	def execute_query(self, query_type: str, key: str = None, value: str = None) -> str:
		queries = ['get', 'set', 'mget']

		if query_type in queries:
			if query_type == 'set':
				try:
					self.db_connection.set(key, value)
					return f"{key} successfully inserted with value {value}"
				except Exception as e:
					print(f"Error inserting key: {e}")
			elif query_type == 'get':
				try:
					return self.db_connection.get(key).decode('utf-8')
				except Exception as e:
					print(f"Error fetching {key}: {e}")
			elif query_type == 'get':
				try:
					return self.db_connection.mget(key)
				except Exception as e:
					print(f"Error fetching {key}: {e}")
		else:
			raise KeyError(f"Invalid query type {query_type}, must be one of {', '.join(queries)}")
