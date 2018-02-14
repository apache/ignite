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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Employee;
import org.apache.ignite.examples.model.EmployeeKey;
import org.apache.ignite.lang.IgniteClosure;

/**
 * This example demonstrates how to use continuous queries together with the transformer APIs.
 * <p>
 * This API can be used to get a notification about cache data changes.
 * User should provide a custom transformer that will transform change event on a remote node.
 * Result of the transformation will be sent over to a local node over the network.
 * That should lead to better network usage and increase performance in case
 * user select only required fields from a complex cache object.
 * </p>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 *
 * @see CacheContinuousQueryExample
 * @see CacheContinuousAsyncQueryExample
 * @see ContinuousQueryWithTransformer
 */
public class CacheContinuousQueryWithTransformerExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheContinuousQueryExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache continuous query with transformer example started.");

            // Auto-close cache at the end of the example.
            try (IgniteCache<EmployeeKey, Employee> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // Create new continuous query with transformer.
                ContinuousQueryWithTransformer<EmployeeKey, Employee, String> qry =
                    new ContinuousQueryWithTransformer<>();

                //Factory to create transformers.
                Factory<IgniteClosure<CacheEntryEvent<? extends EmployeeKey, ? extends Employee>, String>> factory =
                    FactoryBuilder.factoryOf(
                        //Return one field of complex object.
                        //Only this field will be sent over to a local node over the network.
                        (IgniteClosure<CacheEntryEvent<? extends EmployeeKey, ? extends Employee>, String>)
                            event -> event.getValue().name());

                qry.setRemoteTransformerFactory(factory);

                //Listener that will receive transformed data.
                qry.setLocalListener(names -> {
                    for (String name : names)
                        System.out.println("New employee name: " + name);
                });

                // Execute query.
                try (QueryCursor<Cache.Entry<EmployeeKey, Employee>> cur = cache.query(qry)) {
                    populateCache(cache);

                    // Wait for a while while callback is notified about remaining puts.
                    Thread.sleep(2000);
                }
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Populates cache with data.
     *
     * @param cache Employee cache.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static void populateCache(IgniteCache<EmployeeKey, Employee> cache) {
        Map<EmployeeKey, Employee> data = new HashMap<>();

        data.put(new EmployeeKey(1, 1), new Employee(
            "James Wilson",
            12500,
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            Arrays.asList("Human Resources", "Customer Service")
        ));

        data.put(new EmployeeKey(2, 1), new Employee(
            "Daniel Adams",
            11000,
            new Address("184 Fidler Drive, San Antonio, TX", 78130),
            Arrays.asList("Development", "QA")
        ));

        data.put(new EmployeeKey(3, 1), new Employee(
            "Cristian Moss",
            12500,
            new Address("667 Jerry Dove Drive, Florence, SC", 29501),
            Arrays.asList("Logistics")
        ));

        data.put(new EmployeeKey(4, 2), new Employee(
            "Allison Mathis",
            25300,
            new Address("2702 Freedom Lane, San Francisco, CA", 94109),
            Arrays.asList("Development")
        ));

        data.put(new EmployeeKey(5, 2), new Employee(
            "Breana Robbin",
            6500,
            new Address("3960 Sundown Lane, Austin, TX", 78130),
            Arrays.asList("Sales")
        ));

        data.put(new EmployeeKey(6, 2), new Employee(
            "Philip Horsley",
            19800,
            new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
            Arrays.asList("Sales")
        ));

        data.put(new EmployeeKey(7, 2), new Employee(
            "Brian Peters",
            10600,
            new Address("1407 Pearlman Avenue, Boston, MA", 12110),
            Arrays.asList("Development", "QA")
        ));

        cache.putAll(data);
    }
}
