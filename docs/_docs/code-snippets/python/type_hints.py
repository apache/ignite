from pyignite import Client
from pyignite.datatypes import CharObject, ShortObject

client = Client()
client.connect('127.0.0.1', 10800)

my_cache = client.get_or_create_cache('my cache')

my_cache.put('my key', 42)
# Value ‘42’ takes 9 bytes of memory as a LongObject

my_cache.put('my key', 42, value_hint=ShortObject)
# Value ‘42’ takes only 3 bytes as a ShortObject

my_cache.put('a', 1)
# ‘a’ is a key of type String

my_cache.put('a', 2, key_hint=CharObject)
# Another key ‘a’ of type CharObject is created

value = my_cache.get('a')
print(value)  # 1

value = my_cache.get('a', key_hint=CharObject)
print(value)  # 2

# Now let us delete both keys at once
my_cache.remove_keys([
    'a',  # a default type key
    ('a', CharObject),  # a key of type CharObject
])
