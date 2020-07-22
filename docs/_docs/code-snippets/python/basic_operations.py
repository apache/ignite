from pyignite import Client

client = Client()
client.connect('127.0.0.1', 10800)

# Create cache
my_cache = client.create_cache('my cache')

# Put value in cache
my_cache.put('my key', 42)

# Get value from cache
result = my_cache.get('my key')
print(result)  # 42

result = my_cache.get('non-existent key')
print(result)  # None

# Get multiple values from cache
result = my_cache.get_all([
    'my key',
    'non-existent key',
    'other-key',
])
print(result)  # {'my key': 42}
