import logging
import os
from hashlib import md5
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from RedisConnection import RedisConnection
from Command import Command
from dotenv import load_dotenv, find_dotenv

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

load_dotenv(find_dotenv())
_USER: str = os.environ.get('REDIS_USER')
_PASS: str = os.environ.get('PASSWORD')
_HOST_URL: str = os.environ.get('REDIS_PRIMARY_DB')
_QUEUE_URL: str = os.environ.get('REDIS_QUEUE_DB')
_PORT: int = int(os.environ.get('REDIS_PORT'))


def _create_connection(host: str, port: int, passphrase: str, user: str) -> RedisConnection:
	"""Establish a connection with Redis Cloud using provided parameters
	:param host: URL for RedisCloud instance to connect to
	:type host: str
	:param port: RedisCloud instance port number
	:type port: int
	:param passphrase: Redis Cloud password
	:type passphrase: str
	:param user: Redis Cloud username
	:type user: str
	:return: Initialized connection to Redis Cloud
	:rtype: RedisConnection
	"""
	logger.debug("Connecting to Redis Cloud")
	return RedisConnection(host=host, port=port, passphrase=passphrase, user=user)


@app.get("/")
@limiter.limit('60/minute')
async def list_keys(request: Request) -> JSONResponse:
	"""Flush all keys from Redis instance
	:param request: API request
	:type request: Request
	:return: API response with status message
	:rtype: JSONResponse
	"""
	rdb = _create_connection(host=_HOST_URL, port=_PORT, passphrase=_PASS, user=_USER)
	logger.debug("Getting keys from Redis")
	keys = rdb.db_connection.keys()
	response = jsonable_encoder([{"key": value.decode('utf-8')} for value in sorted(keys)])

	return JSONResponse(content=response)


@app.post("/check")
@limiter.limit('120/minute')
async def list_keys(request: Request, cmd: Command) -> JSONResponse:
	"""Flush all keys from Redis instance
	:param request: API request
	:type request: Request
	:param cmd: SQL command
	:type cmd: Command
	:return: API response with status message
	:rtype: JSONResponse
	"""
	hashed_cmd = ''.join(['cmd_', md5(cmd.command_value.encode('utf-8')).hexdigest()])

	rdb = _create_connection(host=_HOST_URL, port=_PORT, passphrase=_PASS, user=_USER)
	keys = rdb.db_connection.keys()

	if hashed_cmd.encode('utf-8') in keys:
		logger.info("Key already exists.")
		return jsonable_encoder({"key": rdb.db_connection.get(hashed_cmd)})
	else:
		logger.info("Adding key to Redis")
		queue_db = _create_connection(host=_QUEUE_URL, port=_PORT, passphrase=_PASS, user=_USER)
		queue_db.db_connection.set(hashed_cmd, cmd.command_value)
		return JSONResponse(content=jsonable_encoder({"msg": "Command added to queue for processing."}))


@app.delete("/remove")
@limiter.limit("120/minute")
async def delete_key(request: Request, cmd: Command) -> JSONResponse:
	"""Delete a key from Redis
	:param request: API request
	:type request: Request
	:param cmd: SQL command
	:type cmd: Command
	:return: API response with status message
	:rtype: JSONResponse
	"""
	hashed_cmd = ''.join(['cmd_', md5(cmd.command_value.encode('utf-8')).hexdigest()])
	rdb = _create_connection(host=_HOST_URL, port=_PORT, passphrase=_PASS, user=_USER)

	try:
		logger.info("Removing key from Redis")
		deleted_count = rdb.db_connection.delete([hashed_cmd])
	except Exception as e:
		logger.error(f"Unable to remove key from Redis: {e}")
		return JSONResponse(content=jsonable_encoder({"err": "Unable to delete key at this time"}))

	return JSONResponse(content=jsonable_encoder({"msg": f"Successfully deleted {deleted_count} keys"}))


@app.delete("/flush")
@limiter.limit('10/minute')
async def flush_db(request: Request) -> JSONResponse:
	"""Flush all keys from Redis instance
	:param request: API request
	:type request: Request
	:return: API response with status message
	:rtype: JSONResponse
	"""

	db = _create_connection(host=_HOST_URL, port=_PORT, passphrase=_PASS, user=_USER)

	try:
		logger.info("Flushing keys from Redis")
		db.db_connection.flushdb()
	except Exception as e:
		logger.error(f"Unable to flush keys from Redis: {e}")
		return JSONResponse(content=jsonable_encoder({"msg": "Unable to flush all keys at this time"}))

	return JSONResponse(content=jsonable_encoder({"msg": "Database cleared"}))


@app.get("/run")
@limiter.limit('120/minute')
async def run_command(request: Request) -> JSONResponse:
	"""Execute the specified command from the Redis instance
	:param request: API request
	:type request: Request
	:return: API response with status message
	:rtype: JSONResponse
	"""
	queue_db = _create_connection(host=_QUEUE_URL, port=_PORT, passphrase=_PASS, user=_USER)

	logger.debug("Checking for earliest key in Redis")
	latest_cmd = queue_db.db_connection.scan(cursor=0, match='cmd_*', count=1)

	logger.debug(f"Starting to execute command")
	cmd_string = latest_cmd[-1][0].decode('utf-8')
	logger.debug(queue_db.db_connection.get(cmd_string))
	queue_db.db_connection.set(cmd_string, 'Processing...')

	logger.debug(queue_db.db_connection.get(cmd_string))

	""" Execute command """

	logger.debug("Command executed, removing key from Redis")
	queue_db.db_connection.delete(cmd_string)

	rdb = _create_connection(host=_HOST_URL, port=_PORT, passphrase=_PASS, user=_USER)
	rdb.db_connection.set(cmd_string, "Jesse Wilson")
	logger.debug(rdb.db_connection.get(cmd_string))
	return JSONResponse(content=jsonable_encoder({"msg": "Finished processing command."}))
