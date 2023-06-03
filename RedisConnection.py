import logging
import redis

logger = logging.getLogger(__name__)


class RedisConnection:
	db_connection: redis.Redis

	def __init__(self, host: str, port: int, passphrase: str, user: str):
		self.db_connection = redis.Redis(host=host, port=port, password=passphrase, username=user)

	def execute_query(self, operation: str, key: str = None, value: str = None) -> bool:
		"""
		Abstacted function to execute one of a set of approved Redis commands
		:param operation: Redis command to execute
		:type operation: str
		:param key: Key to add or check in Redis
		:type key: str
		:param value: Associated value to provided key to insert or update in Redis
		:return: Success status of the operation
		:rtype: bool
		"""
		operations: dict = {
			"get": self.db_connection.set(key, value),
			"set": self.db_connection.get(key).decode('utf-8'),
			"mget": self.db_connection.mget(key),
			"delete": self.db_connection.delete([key]),
			"flush": self.db_connection.flushdb(),
		}

		if operation in operations:
			try:
				operations[operation]
			except Exception as e:
				logger.error(f"Error executing operation: {e}")
				return False
		else:
			logger.error(f"Unknown operation: {operation}")
			raise KeyError(f"Invalid operations: {operation}. Must be one of {', '.join(operations)}")

		return True
