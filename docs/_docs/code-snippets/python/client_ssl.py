from pyignite import Client
import ssl

client = Client(
                use_ssl=True,
                ssl_cert_reqs=ssl.CERT_REQUIRED,
                ssl_keyfile='/path/to/key/file',
                ssl_certfile='/path/to/client/cert',
                ssl_ca_certfile='/path/to/trusted/cert/or/chain',
)

client.connect('localhost', 10800)
