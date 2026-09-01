
import redis
'''
To execute this script, run an Ignite instance with 'redis-ignite-internal-cache-0' cache specified and configured.
You will also need to have 'redis-py' installed.
See https://github.com/andymccurdy/redis-py for the details on redis-py.
'''

r = redis.Redis(host='localhost', port=11212, db=0,decode_responses=True)

# set entry.
r.set('k1', 1)

# check.
print('Value for "k1": %s' % r.get('k1'))

# change entry's value.
r.set('k1', 'new_val 中文')

# check.
print('Value for "k1": %s' % r.get('k1'))

# set another entry.
r.set('k2', 2)

# check.
print('Value for "k2": %s' % r.get('k2'))

# get both values.
print('Values for "k1" and "k2": %s' % r.mget('k1', 'k2'))

# delete one entry.
r.delete('k1')

# check one entry left.
print('Values for "k1" and "k2": %s' % r.mget('k1', 'k2'))

r.hmset('user:1',dict(name='test',group='t1'))

# check one entry left.
print('Values for user:1: %s' % r.hget('user:1','name'))

# check db size
print('Db size: %d' % r.dbsize())

# increment.
print('Value for incremented "inc_k" : %s' % r.incr('inc_k'))

# increment again.
print('Value for incremented "inc_k" : %s' % r.incr('inc_k'))
