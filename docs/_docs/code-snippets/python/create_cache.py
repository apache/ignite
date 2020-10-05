from pyignite import Client

# Open a connection
client = Client()
client.connect('127.0.0.1', 10800)

# Create a cache
my_cache = client.create_cache('myCache')
