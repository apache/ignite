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
package org.apache.ignite.snippets;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.Test;

public class SqlAPI {

    class Person implements Serializable {
        /** Indexed field. Will be visible to the SQL engine. */
        @QuerySqlField(index = true)
        private long id;

        /** Queryable field. Will be visible to the SQL engine. */
        @QuerySqlField
        private String name;

        /** Will NOT be visible to the SQL engine. */
        private int age;

        /**
         * Indexed field sorted in descending order. Will be visible to the SQL engine.
         */
        @QuerySqlField(index = true, descending = true)
        private float salary;
    }

    void cancellingByTimeout() {
        // tag::set-timeout[]
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT * from Person");

        // Setting query execution timeout
        query.setTimeout(10_000, TimeUnit.SECONDS);

        // end::set-timeout[]
    }

    void cancellingByCallingClose(IgniteCache<Long, Person> cache) {
        // tag::cancel-by-closing[]
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT * FROM Person");

        // Executing the query
        QueryCursor<List<?>> cursor = cache.query(query);

        // Halting the query that might be still in progress.
        cursor.close();

        // end::cancel-by-closing[]
    }

    void enforceJoinOrder() {

        // tag::enforceJoinOrder[]
        SqlFieldsQuery query = new SqlFieldsQuery(
                "SELECT * FROM TABLE_A, TABLE_B USE INDEX(HASH_JOIN_IDX)"
                        + " WHERE TABLE_A.column1 = TABLE_B.column2").setEnforceJoinOrder(true);
        // end::enforceJoinOrder[]
    }

    void simpleQuery(Ignite ignite) {
        // tag::simple-query[]
        IgniteCache<Long, Person> cache = ignite.cache("Person");

        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select concat(firstName, ' ', lastName) from Person");

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            for (List<?> row : cursor)
                System.out.println("personName=" + row.get(0));
        }
        // end::simple-query[]
    }

    void insert(Ignite ignite) {
        // tag::insert[]
        IgniteCache<Long, Person> cache = ignite.cache("personCache");

        cache.query(
                new SqlFieldsQuery("INSERT INTO Person(id, firstName, lastName) VALUES(?, ?, ?)")
                        .setArgs(1L, "John", "Smith"))
                .getAll();

        // end::insert[]

    }

    void update(Ignite ignite) {
        // tag::update[]
        IgniteCache<Long, Person> cache = ignite.cache("personCache");

        cache.query(new SqlFieldsQuery("UPDATE Person set lastName = ? " + "WHERE id >= ?")
                .setArgs("Jones", 2L)).getAll();
        // end::update[]
    }

    void delete(Ignite ignite) {
        // tag::delete[]
        IgniteCache<Long, Person> cache = ignite.cache("personCache");

        cache.query(new SqlFieldsQuery("DELETE FROM Person " + "WHERE id >= ?").setArgs(2L))
                .getAll();

        // end::delete[]
    }

    void merge(Ignite ignite) {
        // tag::merge[]
        IgniteCache<Long, Person> cache = ignite.cache("personCache");

        cache.query(new SqlFieldsQuery("MERGE INTO Person(id, firstName, lastName)"
                + " values (1, 'John', 'Smith'), (5, 'Mary', 'Jones')")).getAll();
        // end::merge[]
    }

    void setSchema() {
        // tag::set-schema[]
        SqlFieldsQuery sql = new SqlFieldsQuery("select name from City").setSchema("PERSON");
        // end::set-schema[]
    }

    void createTable(Ignite ignite) {
        // tag::create-table[]
        IgniteCache<Long, Person> cache = ignite
                .getOrCreateCache(new CacheConfiguration<Long, Person>().setName("Person"));

        // Creating City table.
        cache.query(new SqlFieldsQuery(
                "CREATE TABLE City (id int primary key, name varchar, region varchar)")).getAll();
        // end::create-table[]
    }

    // tag::sql-function-example[]
    static class SqlFunctions {
        @QuerySqlFunction
        public static int sqr(int x) {
            return x * x;
        }
    }

    // end::sql-function-example[]

    // tag::sql-table-function-example[]
    static class SqlTableFunctions {
        @QuerySqlTableFunction(columnTypes = {Integer.class, String.class}, columnNames = {"INT_COL", "STR_COL"})
        public static Iterable<Object[]> table_function(int i) {
            return Arrays.asList(
                new Object[]{i, "" + i},
                new Object[]{i * 10, "empty"}
            );
        }
    }
    // end::sql-table-function-example[]

    @Test
    IgniteCache setSqlFunction(Ignite ignite) {

        // tag::sql-function-config[]
        // Preparing a cache configuration.
        CacheConfiguration cfg = new CacheConfiguration("myCache");

        // Registering the class that contains custom SQL functions.
        cfg.setSqlFunctionClasses(SqlFunctions.class);

        IgniteCache cache = ignite.createCache(cfg);
        // end::sql-function-config[]

        return cache;
    }

    @Test
    IgniteCache testSqlTableFunction(Ignite ignite) {
        // tag::sql-table-function-config-query[]
        CacheConfiguration cfg = new CacheConfiguration("myCache");

        cfg.setSqlFunctionClasses(SqlTableFunctions.class);

        IgniteCache cache = ignite.createCache(cfg);

        SqlFieldsQuery query = new SqlFieldsQuery("SELECT STR_COL FROM TABLE_FUNCTION(10) WHERE INT_COL > 50");

        cache.query(query).getAll();
        // end::sql-table-function-config-query[]

        return cache;
    }

    void call(IgniteCache cache) {

        // tag::sql-function-query[]
        // Preparing the query that uses the custom defined 'sqr' function.
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT name FROM myCache WHERE sqr(size) > 100");

        // Executing the query.
        cache.query(query).getAll();

        // end::sql-function-query[]
    }
}
