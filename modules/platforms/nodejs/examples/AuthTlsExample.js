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

const FS = require('fs');
const IgniteClient = require('apache-ignite-client');
const ObjectType = IgniteClient.ObjectType;
const ComplexObjectType = IgniteClient.ComplexObjectType;
const BinaryObject = IgniteClient.BinaryObject;
const CacheEntry = IgniteClient.CacheEntry;
const ScanQuery = IgniteClient.ScanQuery;
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

const ENDPOINT = 'localhost:10800';
const USER_NAME = 'ignite';
const PASSWORD = 'ignite';

const TLS_KEY_FILE_NAME = __dirname + '/certs/client.key';
const TLS_CERT_FILE_NAME = __dirname + '/certs/client.crt';
const TLS_CA_FILE_NAME = __dirname + '/certs/ca.crt';

const CACHE_NAME = 'AuthTlsExample_cache';

// This example demonstrates how to establish a secure connection to an Ignite node and use username/password authentication,
// as well as basic Key-Value Queries operations for primitive types:
// - connects to a node using TLS and providing username/password
// - creates a cache, if it doesn't exist
//   - specifies key and value type of the cache
// - put data of primitive types into the cache
// - get data from the cache
// - destroys the cache
class AuthTlsExample {

    async start() {
        const igniteClient = new IgniteClient(this.onStateChanged.bind(this));
        try {
            const connectionOptions = {
                'key' : FS.readFileSync(TLS_KEY_FILE_NAME),
                'cert' : FS.readFileSync(TLS_CERT_FILE_NAME),
                'ca' : FS.readFileSync(TLS_CA_FILE_NAME)
            };
            await igniteClient.connect(new IgniteClientConfiguration(ENDPOINT).
                setUserName(USER_NAME).
                setPassword(PASSWORD).
                setConnectionOptions(true, connectionOptions));

            const cache = (await igniteClient.getOrCreateCache(CACHE_NAME)).
                setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER).
                setValueType(ObjectType.PRIMITIVE_TYPE.SHORT_ARRAY);

            await this.putGetData(cache);

            await igniteClient.destroyCache(CACHE_NAME);
        }
        catch (err) {
            console.log('ERROR: ' + err.message);
        }
        finally {
            igniteClient.disconnect();
        }
    }

    async putGetData(cache) {
        let keys = [1, 2, 3];
        let values = keys.map(key => this.generateValue(key));

        // put multiple values in parallel
        await Promise.all([
            await cache.put(keys[0], values[0]),
            await cache.put(keys[1], values[1]),
            await cache.put(keys[2], values[2])
        ]);
        console.log('Cache values put successfully');

        // get values sequentially
        let value;
        for (let i = 0; i < keys.length; i++) {
            value = await cache.get(keys[i]);
            if (!this.compareValues(value, values[i])) {
                console.log('Unexpected cache value!');
                return;
            }
        }
        console.log('Cache values get successfully');
    }

    compareValues(array1, array2) {
        return array1.length === array2.length &&
            array1.every((value1, index) => value1 === array2[index]);
    }

    generateValue(key) {
        const length = key + 5;
        const result = new Array(length);
        for (let i = 0; i < length; i++) {
            result[i] = key * 10 + i;
        }
        return result;
    }

    onStateChanged(state, reason) {
        if (state === IgniteClient.STATE.CONNECTED) {
            console.log('Client is started');
        }
        else if (state === IgniteClient.STATE.DISCONNECTED) {
            console.log('Client is stopped');
            if (reason) {
                console.log(reason);
            }
        }
    }
}

const authTlsExample = new AuthTlsExample();
authTlsExample.start();
