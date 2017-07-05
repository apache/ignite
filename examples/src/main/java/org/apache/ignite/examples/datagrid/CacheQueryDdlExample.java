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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Example to showcase DDL capabilities of Ignite's SQL engine.
 */
public class CacheQueryDdlExample {
    /** Dummy cache name. */
    private static final String DUMMY_IDX_CACHE = "idx_cache";

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

            // We need to create a dummy cache with indexing enabled in order to have API entry point
            // for running the queries, but this will change in the future.
            CacheConfiguration<?, ?> idxCacheCfg = new CacheConfiguration<>(DUMMY_IDX_CACHE);
            idxCacheCfg.setIndexedTypes(Integer.class, Integer.class);

            // Auto-close cache at the end of the example.
            try (
                IgniteCache<?, ?> idxCache = ignite.getOrCreateCache(idxCacheCfg)
            ) {
                // Let's create one table based on actual cache template...
                execute(ignite, "CREATE TABLE Person (id int primary key, name varchar, surname varchar, age int, " +
                    "orgId int, city varchar) WITH \"template=partitioned,backups=1\"");

                // ...and another one based
                execute(ignite, "CREATE TABLE City (name varchar primary key, region varchar, population int) " +
                    "WITH \"template=replicated\"");

                print("Tables have been created.");

                for (Object[] person : new Object[][] {
                    new Object[]{1L, 1L, "John", "Doe", 40, "Forest Hill"},
                    new Object[]{2L, 2L, "Jane", "Roe", 20, "Washington"},
                    new Object[]{3L, 2L, "Mary", "Major", 50, "Denver"},
                    new Object[]{4L, 1L, "Richard", "Miles", 30, "New York"}
                }) {
                    execute(ignite,
                        "insert into Person (id, orgId, name, surname, age, city) values (?, ?, ?, ?, ?, ?)",
                        person);
                }

                for (Object[] city : new Object[][]{
                    new Object[]{"Forest Hill", "Maryland", 300000},
                    new Object[]{"Denver", "Colorado", 600000},
                    new Object[]{"St. Petersburg", "Texas", 400000}
                }) {
                    execute(ignite,
                        "insert into City (name, region, population) values (?, ?, ?)",
                        city);
                }

                print("Data has been inserted.");

                execute(ignite, "create index persIdx on Person (city, age desc)");

                print("Index has been created.");

                List<List<?>> res = execute(ignite,
                    "SELECT p.name, p.surname, c.name from person p inner join City c on c.name = p.city");

                print("Query results:");

                for (Object next : res)
                    System.out.println(">>>     " + next);

                execute(ignite, "drop index persIdx");

                print("Index has been dropped.");

                execute(ignite, "drop table person");

                execute(ignite, "drop table \"CITY\"");

                print("Tables have been dropped.");
            }
            finally {
                // Distributed cache can be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(DUMMY_IDX_CACHE);
            }

            print("Cache query DDL example finished.");
        }
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
     * Run a query with parameters on PUBLIC schema.
     * @param ignite Ignite.
     * @param sql Statement.
     * @param args Arguments.
     * @return Result.
     */
    private static List<List<?>> execute(Ignite ignite, String sql, Object... args) {
        return ignite.cache(DUMMY_IDX_CACHE).query(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args)).getAll();
    }
}
