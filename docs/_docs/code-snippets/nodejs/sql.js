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
const CacheConfiguration = IgniteClient.CacheConfiguration;
const QueryEntity = IgniteClient.QueryEntity;
const QueryField = IgniteClient.QueryField;
const ObjectType = IgniteClient.ObjectType;
const ComplexObjectType = IgniteClient.ComplexObjectType;
const CacheEntry = IgniteClient.CacheEntry;
const SqlQuery = IgniteClient.SqlQuery;

async function performSqlQuery() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
	  	//tag::sql[]
        // Cache configuration required for sql query execution
        const cacheConfiguration = new CacheConfiguration().
            setQueryEntities(
                new QueryEntity().
                    setValueTypeName('Person').
                    setFields([
                        new QueryField('name', 'java.lang.String'),
                        new QueryField('salary', 'java.lang.Double')
                    ]));
        const cache = (await igniteClient.getOrCreateCache('sqlQueryPersonCache', cacheConfiguration)).
            setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER).
            setValueType(new ComplexObjectType({ 'name' : '', 'salary' : 0 }, 'Person'));

        // Put multiple values using putAll()
        await cache.putAll([
            new CacheEntry(1, { 'name' : 'John Doe', 'salary' : 1000 }),
            new CacheEntry(2, { 'name' : 'Jane Roe', 'salary' : 2000 }),
            new CacheEntry(3, { 'name' : 'Mary Major', 'salary' : 1500 })]);

        // Create and configure sql query
        const sqlQuery = new SqlQuery('Person', 'salary > ? and salary <= ?').
            setArgs(900, 1600);
        // Obtain sql query cursor
        const cursor = await cache.query(sqlQuery);
        // Get all cache entries returned by the sql query
        for (let cacheEntry of await cursor.getAll()) {
            console.log(cacheEntry.getValue());
        }

	  	//end::sql[]

        await igniteClient.destroyCache('sqlQueryPersonCache');
    }
    catch (err) {
        console.log(err.message);
    }
    finally {
        igniteClient.disconnect();
    }
}

performSqlQuery();
//end::example-block[]
