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

package org.apache.ignite.examples.datagrid.store.auto;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.examples.datagrid.store.*;
import org.h2.jdbcx.*;
import org.h2.tools.*;

import java.io.*;
import java.sql.*;

/**
 * Demonstrates how to load data from database.
 * <p>
 * This example uses {@link CacheJdbcPojoStore} as a persistent store.
 * <p>
 * To run this example your should start {@link H2Startup} first.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheAutoStoreLoadDataExample {
    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

    /** */
    private static final String DB_SCRIPT =
        "delete from PERSON;\n" +
        "insert into PERSON(id, first_name, last_name) values(1, 'Johannes', 'Kepler');\n" +
        "insert into PERSON(id, first_name, last_name) values(2, 'Galileo', 'Galilei');\n" +
        "insert into PERSON(id, first_name, last_name) values(3, 'Henry', 'More');\n" +
        "insert into PERSON(id, first_name, last_name) values(4, 'Polish', 'Brethren');\n" +
        "insert into PERSON(id, first_name, last_name) values(5, 'Robert', 'Boyle');\n" +
        "insert into PERSON(id, first_name, last_name) values(6, 'Isaac', 'Newton');";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        initializeDatabase();

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache auto store load data example started.");

            CacheConfiguration<Long, Person> cacheCfg = CacheConfig.jdbcPojoStoreCache();

            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheCfg)) {
                System.out.println(">>> Load cache from database using custom SQL.");

                System.out.println(">>> Cache size: " + cache.size());

                long start = System.currentTimeMillis();

                // Start loading cache from persistent store on all caching nodes.
                cache.loadCache(null, "java.lang.Long", "select * from PERSON where id <= 3");

                long end = System.currentTimeMillis();

                System.out.println(">>> Loaded " + cache.size() + " keys with backups in " + (end - start) + "ms.");

                System.out.println(">>> Load cache data from database.");

                cache.clear();

                System.out.println(">>> Cache size: " + cache.size());

                start = System.currentTimeMillis();

                // Start loading cache from persistent store on all caching nodes.
                cache.loadCache(null);

                end = System.currentTimeMillis();

                System.out.println(">>> Loaded " + cache.size() + " keys with backups in " + (end - start) + "ms.");
            }
        }
    }

    /**
     * Prepares database for load example execution.
     *
     * @throws IgniteException If failed.
     */
    private static void initializeDatabase() throws IgniteException {
        try {
            // Try to connect to database server.
            JdbcConnectionPool dataSrc = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", "");

            // Load sample data into database.
            RunScript.execute(dataSrc.getConnection(), new StringReader(DB_SCRIPT));
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to initialize database", e);
        }
    }
}
