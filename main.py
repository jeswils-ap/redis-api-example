import uvicorn
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
_KEY_ = os.environ.get('CA_KEY')
_CERT_ = os.environ.get('CA_CERT')


if __name__ == '__main__':
	uvicorn.run(
		'queue_test:app', port=443, host='127.0.0.1',
		ssl_keyfile=_KEY_,
		ssl_certfile=_CERT_
	)
