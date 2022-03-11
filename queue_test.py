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

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

load_dotenv(find_dotenv())
_USER_ = os.environ.get('REDIS_USER')
_PASS_ = os.environ.get('PASSWORD')
_HOST_URL_ = os.environ.get('REDIS_PRIMARY_DB')
_QUEUE_URL_ = os.environ.get('REDIS_QUEUE_DB')
_PORT_ = os.environ.get('REDIS_PORT')


def create_connection(host: str, port: int, passphrase: str, user: str):
	return RedisConnection(host=host, port=port, passphrase=passphrase, user=user)


@app.get("/")
@limiter.limit('60/minute')
async def list_keys(request: Request):
	rdb = create_connection(host=_HOST_URL_, port=_PORT_, passphrase=_PASS_, user=_USER_)
	keys = rdb.db_connection.keys()
	response = jsonable_encoder([{"key": value.decode('utf-8')} for value in sorted(keys)])

	return JSONResponse(content=response)


@app.post("/check")
@limiter.limit('120/minute')
async def list_keys(request: Request, cmd: Command):
	hashed_cmd = ''.join(['cmd_', md5(cmd.command_value.encode('utf-8')).hexdigest()])

	rdb = create_connection(host=_HOST_URL_, port=_PORT_, passphrase=_PASS_, user=_USER_)
	keys = rdb.db_connection.keys()

	if hashed_cmd.encode('utf-8') in keys:
		return rdb.db_connection.get(hashed_cmd)
	else:
		queue_db = create_connection(host=_QUEUE_URL_, port=_PORT_, passphrase=_PASS_, user=_USER_)
		queue_db.db_connection.set(hashed_cmd, cmd.command_value)
		return f"Command added to queue for processing."


@app.delete("/flush")
@limiter.limit('10/minute')
async def flush_db(request: Request):
	db = create_connection(host=_HOST_URL_, port=_PORT_, passphrase=_PASS_, user=_USER_)
	db.db_connection.flushdb()

	return f"Database cleared"


@app.get("/run")
@limiter.limit('120/minute')
async def run_command(request: Request):
	queue_db = create_connection(host=_QUEUE_URL_, port=_PORT_, passphrase=_PASS_, user=_USER_)
	latest_cmd = queue_db.db_connection.scan(cursor=0, match='cmd_*', count=1)

	cmd_string = latest_cmd[-1][0].decode('utf-8')
	print(queue_db.db_connection.get(cmd_string))
	queue_db.db_connection.set(cmd_string, 'Processing...')

	print(queue_db.db_connection.get(cmd_string))

	""" Execute command """

	queue_db.db_connection.delete(cmd_string)

	rdb = create_connection(host=_HOST_URL_, port=_PORT_, passphrase=_PASS_, user=_USER_)
	rdb.db_connection.set(cmd_string, "Jesse Wilson")
	print(rdb.db_connection.get(cmd_string))
	return f"Finished processing command."
