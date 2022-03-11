import uvicorn

if __name__ == '__main__':
	uvicorn.run(
		'queue_test:app', port=443, host='127.0.0.1',
		ssl_keyfile='/Users/jessewilson/Downloads/PyCharm/certs/domain.key',
		ssl_certfile='/Users/jessewilson/Downloads/PyCharm/certs/domain.crt'
	)
