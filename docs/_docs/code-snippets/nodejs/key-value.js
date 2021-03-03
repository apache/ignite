/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//tag::example-block[]
const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const ObjectType = IgniteClient.ObjectType;
const CacheEntry = IgniteClient.CacheEntry;

async function performCacheKeyValueOperations() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
        const cache = (await igniteClient.getOrCreateCache('myCache')).
        setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);
        // Put and get value
        await cache.put(1, 'abc');
        const value = await cache.get(1);
        console.log(value);

        // Put and get multiple values using putAll()/getAll() methods
        await cache.putAll([new CacheEntry(2, 'value2'), new CacheEntry(3, 'value3')]);
        const values = await cache.getAll([1, 2, 3]);
        console.log(values.flatMap(val => val.getValue()));

        // Removes all entries from the cache
        await cache.clear();
    }
    catch (err) {
        console.log(err.message);
    }
    finally {
        igniteClient.disconnect();
    }
}

performCacheKeyValueOperations();
//end::example-block[]
