from pyignite import Client
import ssl

#tag::no-ssl[]
client = Client(username='ignite', password='ignite', use_ssl=False)
#end::no-ssl[]

client = Client(
                ssl_cert_reqs=ssl.CERT_REQUIRED,
                ssl_keyfile='/path/to/key/file',
                ssl_certfile='/path/to/client/cert',
                ssl_ca_certfile='/path/to/trusted/cert/or/chain',
                username='ignite',
                password='ignite',)

client.connect('localhost', 10800)
