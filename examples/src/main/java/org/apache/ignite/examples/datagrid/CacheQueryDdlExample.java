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
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * Example to showcase DDL capabilities of Ignite's SQL engine.
 * <p>
 * Remote nodes could be started from command line as follows:
 * {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in either same or another JVM.
 */
public class CacheQueryDdlExample {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

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

            // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
            // will appear in future versions, JDBC and ODBC drivers do not require it already).
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME)
                .setSqlSchema("PUBLIC").setIndexedTypes(Integer.class, Integer.class);

            try (
                IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg)
            ) {
                // Create table based on PARTITIONED template with one backup.
                cache.query(new SqlFieldsQuery("CREATE TABLE Person (id int primary key, name varchar, " +
                    "surname varchar, age int, orgId int, city varchar) WITH \"template=partitioned,backups=1\""))
                    .getAll();

                // Create reference City table based on REPLICATED template.
                cache.query(new SqlFieldsQuery("CREATE TABLE City (name varchar primary key, region varchar, " +
                    "population int) WITH \"template=replicated\"")).getAll();

                // Create an index.
                cache.query(new SqlFieldsQuery("create index persIdx on Person (city, age desc)")).getAll();

                print("Created database objects.");

                SqlFieldsQuery qry = new SqlFieldsQuery("insert into Person (id, orgId, name, surname, age, city) " +
                    "values (?, ?, ?, ?, ?, ?)");

                cache.query(qry.setArgs(1L, 1L, "John", "Doe", 40, "Forest Hill")).getAll();
                cache.query(qry.setArgs(2L, 2L, "Jane", "Roe", 20, "Washington")).getAll();
                cache.query(qry.setArgs(3L, 2L, "Mary", "Major", 50, "Denver")).getAll();
                cache.query(qry.setArgs(4L, 1L, "Richard", "Miles", 30, "New York")).getAll();

                qry = new SqlFieldsQuery("insert into City (name, region, population) values (?, ?, ?)");

                cache.query(qry.setArgs("Forest Hill", "Maryland", 300000)).getAll();
                cache.query(qry.setArgs("Denver", "Colorado", 600000)).getAll();
                cache.query(qry.setArgs("St. Petersburg", "Texas", 400000)).getAll();

                print("Populated data.");

                List<List<?>> res = cache.query(new SqlFieldsQuery(
                    "SELECT p.name, p.surname, c.name from person p inner join City c on c.name = p.city")).getAll();

                print("Query results:");

                for (Object next : res)
                    System.out.println(">>>     " + next);

                cache.query(new SqlFieldsQuery("drop table Person")).getAll();
                cache.query(new SqlFieldsQuery("drop table City")).getAll();

                print("Dropped database objects.");
            }
            finally {
                // Distributed cache can be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(DUMMY_CACHE_NAME);
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
}
