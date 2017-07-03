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

package org.apache.ignite.examples.datagrid;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Organization;

/**
 * Example to showcase DDL capabilities of Ignite's SQL engine.
 */
public class CacheQueryDdlExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryDdlExample.class.getSimpleName() + "Organizations";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    @SuppressWarnings({"unused", "ThrowFromFinallyBlock"})
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            print("Cache query DDL example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            ignite.addCacheConfiguration(new CacheConfiguration<>("TEMPLATE_CACHE")
                .setCacheMode(CacheMode.REPLICATED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            );

            // Auto-close cache at the end of the example.
            try (
                IgniteCache<Long, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg)
            ) {
                // Let's create one table based on actual cache template...
                execute(ignite, "CREATE TABLE Person (id int primary key, name varchar, surname varchar, age int, " +
                    "orgId int, city varchar) WITH \"template=TEMPLATE_CACHE\"");

                // ...and another one based
                execute(ignite, "CREATE TABLE City (name varchar primary key, region varchar, population int) " +
                    "WITH \"template=partitioned,atomicity=atomic,backups=3\"");

                execute(ignite, "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar)");

                print("Tables have been created.");

                // Now let's put some data into tables
                insert(ignite, "insert into Organization (_key, id, name) values (?, ?, ?)", ORG_CACHE,
                    new Object[] {1L, 1L, "ASF"},
                    new Object[] {2L, 2L, "Eclipse"});

                insert(ignite,
                    "insert into Person (id, orgId, name, surname, age, city) values (?, ?, ?, ?, ?, ?)",
                    new Object[] { 1L, 1L, "John", "Doe", 40, "Forest Hill" },
                    new Object[] { 2L, 2L, "Jane", "Roe", 20, "Washington" },
                    new Object[] { 3L, 2L, "Mary", "Major", 50, "Denver"},
                    new Object[] { 4L, 1L, "Richard", "Miles", 30, "New York"}
                );

                insert(ignite,
                    "insert into City (name, region, population) values (?, ?, ?)",
                    new Object[] { "Forest Hill", "Maryland", 300000 },
                    new Object[] { "Denver", "Colorado", 600000 },
                    new Object[] { "St. Petersburg", "Texas", 400000 }
                );

                print("Data has been inserted.");

                // And now let's check it's all there
                select(ignite, "Found people before indexes were created:");

                // Now let's drop some indexes...
                execute(ignite, "create index orgIdx on Organization (name desc)", ORG_CACHE);

                execute(ignite, "create index persIdx on Person (city, age desc)");

                execute(ignite, "create index if not exists persIdx on Person (name, surname)");

                execute(ignite, "create index cityIdx on City (population)");

                print("Indexes have been created.");

                // And check that data is the same, with indexes or without
                select(ignite, "Found people after indexes have been created:");

                print("Data has been modified.");

                // Let's modify the data and see how it affects SELECT results
                execute(ignite, "delete from Organization where name = 'Eclipse'", ORG_CACHE);

                select(ignite, "Found people after data modification:");

                // Now let's drop the indexes...
                execute(ignite, "drop index orgIdx", ORG_CACHE);

                execute(ignite, "drop index persIdx");

                execute(ignite, "drop index if exists missingIdx");

                execute(ignite, "drop index cityIdx");

                print("Indexes have been dropped.");

                // And check that data is the same, with indexes or without
                select(ignite, "Found people after indexes have been dropped:");

                execute(ignite, "drop table person");

                execute(ignite, "drop table \"CITY\"");

                execute(ignite, "drop table if exists MissingTable");

                print("Tables have been dropped.");
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query DDL example finished.");
        }
    }

    /**
     * Query current data.
     *
     * @param ignite Ignite.
     * @param msg Message.
     */
    private static void select(Ignite ignite, String msg) {
        String sql = "SELECT p.name, p.surname, o.name, c.name from PUBLIC.person p " +
            "inner join Organization o on p.orgId = o.id inner join PUBLIC.City c on c.name = p.city";

        List<List<?>> res = execute(ignite, sql, ORG_CACHE);

        print(msg);

        for (Object next : res)
            System.out.println(">>>     " + next);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Run a parameterless query on PUBLIC schema.
     * @param ignite Ignite.
     * @param sql Statement.
     * @return Result.
     */
    private static List<List<?>> execute(Ignite ignite, String sql) {
        return execute(ignite, sql, (Object[])null);
    }

    /**
     * Run a query with parameters on specified schema.
     * @param ignite Ignite.
     * @param sql Statement.
     * @param schema Schema name.
     * @param args Arguments.
     * @return Result.
     */
    private static List<List<?>> execute(Ignite ignite, String sql, String schema, Object... args) {
        return ignite.cache(ORG_CACHE).query(new SqlFieldsQuery(sql).setSchema(schema).setArgs(args)).getAll();
    }

    /**
     * Run a query with parameters on PUBLIC schema.
     * @param ignite Ignite.
     * @param sql Statement.
     * @param args Arguments.
     * @return Result.
     */
    private static List<List<?>> execute(Ignite ignite, String sql, Object... args) {
        return execute(ignite, sql, "PUBLIC", args);
    }

    /**
     * Insert rows into table on PUBLIC schema.
     * @param ignite Ignite.
     * @param sql Statement.
     * @param args Arguments.
     */
    private static void insert(Ignite ignite, String sql, Object[]... args) {
        insert(ignite, sql, "PUBLIC", args);
    }

    /**
     * Insert rows into table on specified schema.
     * @param ignite Ignite.
     * @param sql Statement.
     * @param schema Schema name.
     * @param args Arguments.
     */
    private static void insert(Ignite ignite, String sql, String schema, Object[]... args) {
        for (Object[] singleArgs : args)
            execute(ignite, sql, schema, singleArgs);
    }
}
