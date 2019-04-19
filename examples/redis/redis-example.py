#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
# 
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import redis
'''
To execute this script, run an Ignite instance with 'redis-ignite-internal-cache-0' cache specified and configured.
You will also need to have 'redis-py' installed.
See https://github.com/andymccurdy/redis-py for the details on redis-py.

See https://apacheignite.readme.io/docs/redis for more details on Redis integration.
'''

r = redis.StrictRedis(host='localhost', port=11211, db=0)

# set entry.
r.set('k1', 1)

# check.
print('Value for "k1": %s' % r.get('k1'))

# change entry's value.
r.set('k1', 'new_val')

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

# check db size
print('Db size: %d' % r.dbsize())

# increment.
print('Value for incremented "inc_k" : %s' % r.incr('inc_k'))

# increment again.
print('Value for incremented "inc_k" : %s' % r.incr('inc_k'))
