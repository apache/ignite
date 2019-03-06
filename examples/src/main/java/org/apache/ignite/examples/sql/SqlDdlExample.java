/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.sql;

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
public class SqlDdlExample {
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
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");

            try (
                IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg)
            ) {
                // Create reference City table based on REPLICATED template.
                cache.query(new SqlFieldsQuery(
                    "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll();

                // Create table based on PARTITIONED template with one backup.
                cache.query(new SqlFieldsQuery(
                    "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                    "WITH \"backups=1, affinity_key=city_id\"")).getAll();

                // Create an index.
                cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll();

                print("Created database objects.");

                SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)");

                cache.query(qry.setArgs(1L, "Forest Hill")).getAll();
                cache.query(qry.setArgs(2L, "Denver")).getAll();
                cache.query(qry.setArgs(3L, "St. Petersburg")).getAll();

                qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)");

                cache.query(qry.setArgs(1L, "John Doe", 3L)).getAll();
                cache.query(qry.setArgs(2L, "Jane Roe", 2L)).getAll();
                cache.query(qry.setArgs(3L, "Mary Major", 1L)).getAll();
                cache.query(qry.setArgs(4L, "Richard Miles", 2L)).getAll();

                print("Populated data.");

                List<List<?>> res = cache.query(new SqlFieldsQuery(
                    "SELECT p.name, c.name FROM Person p INNER JOIN City c on c.id = p.city_id")).getAll();

                print("Query results:");

                for (Object next : res)
                    System.out.println(">>>    " + next);

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
