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
const ScanQuery = IgniteClient.ScanQuery;

async function performScanQuery() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
        const cache = (await igniteClient.getOrCreateCache('myCache')).setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);

        // Put multiple values using putAll()
        await cache.putAll([
            new CacheEntry(1, 'value1'),
            new CacheEntry(2, 'value2'),
            new CacheEntry(3, 'value3')]);

        // Create and configure scan query
        const scanQuery = new ScanQuery()
            .setPageSize(1);
        // Obtain scan query cursor
        const cursor = await cache.query(scanQuery);
        // Get all cache entries returned by the scan query
        for (let cacheEntry of await cursor.getAll()) {
            console.log(cacheEntry.getValue());
        }

        await igniteClient.destroyCache('myCache');
    } catch (err) {
        console.log(err.message);
    } finally {
        igniteClient.disconnect();
    }
}

performScanQuery();
//end::example-block[]
