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
const ObjectType = IgniteClient.ObjectType;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;

async function performSqlFieldsQuery() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
        const cache = await igniteClient.getOrCreateCache('myPersonCache', new CacheConfiguration()
            .setSqlSchema('PUBLIC'));

        // Create table using SqlFieldsQuery
        (await cache.query(new SqlFieldsQuery(
            'CREATE TABLE Person (id INTEGER PRIMARY KEY, firstName VARCHAR, lastName VARCHAR, salary DOUBLE)'))).getAll();

        // Insert data into the table
        const insertQuery = new SqlFieldsQuery('INSERT INTO Person (id, firstName, lastName, salary) values (?, ?, ?, ?)')
            .setArgTypes(ObjectType.PRIMITIVE_TYPE.INTEGER);
        (await cache.query(insertQuery.setArgs(1, 'John', 'Doe', 1000))).getAll();
        (await cache.query(insertQuery.setArgs(2, 'Jane', 'Roe', 2000))).getAll();

        // Obtain sql fields cursor
        const sqlFieldsCursor = await cache.query(
            new SqlFieldsQuery("SELECT concat(firstName, ' ', lastName), salary from Person").setPageSize(1));

        // Iterate over elements returned by the query
        do {
            console.log(await sqlFieldsCursor.getValue());
        } while (sqlFieldsCursor.hasMore());

        // Drop the table
        (await cache.query(new SqlFieldsQuery("DROP TABLE Person"))).getAll();
    } catch (err) {
        console.log(err.message);
    } finally {
        igniteClient.disconnect();
    }
}

performSqlFieldsQuery();
//end::example-block[]
