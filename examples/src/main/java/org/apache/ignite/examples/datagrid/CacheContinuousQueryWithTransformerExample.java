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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.OrganizationType;

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
    private static final String CACHE_NAME = CacheContinuousQueryWithTransformerExample.class.getSimpleName();

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
            try (IgniteCache<Integer, Organization> cache = ignite.getOrCreateCache(CACHE_NAME).withKeepBinary()) {
                // Create new continuous query with transformer.
                ContinuousQueryWithTransformer<Integer, Organization, String> qry =
                    new ContinuousQueryWithTransformer<>();

                // Factory to create transformers.
                qry.setRemoteTransformerFactory(() -> {
                    // Return one field of complex object.
                    // Only this field will be sent over to a local node over the network.
                    return event -> ((BinaryObject)event.getValue()).field("name");
                });

                // Listener that will receive transformed data.
                qry.setLocalListener(names -> {
                    for (String name : names)
                        System.out.println("New organization name: " + name);
                });

                // Execute query.
                try (QueryCursor<Cache.Entry<Integer, Organization>> cur = cache.query(qry)) {
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
     * @param cache Organization cache.
     */
    private static void populateCache(IgniteCache<Integer, Organization> cache) {
        Map<Integer, Organization> data = new HashMap<>();

        data.put(1, new Organization(
            "Microsoft", // Name.
            new Address("1096 Eddy Street, San Francisco, CA", 94109), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis()))); // Last update time.

        data.put(2, new Organization(
            "Red Cross", // Name.
            new Address("184 Fidler Drive, San Antonio, TX", 78205), // Address.
            OrganizationType.NON_PROFIT, // Type.
            new Timestamp(System.currentTimeMillis()))); // Last update time.

        data.put(3, new Organization(
            "Apple", // Name.
            new Address("1 Infinite Loop, Cupertino, CA", 95014), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis()))); // Last update time.

        data.put(4, new Organization(
            "IBM", // Name.
            new Address("1 New Orchard Road Armonk, New York", 10504), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis()))); // Last update time.

        data.put(5, new Organization(
            "NASA Armstrong Flight Research Center", // Name.
            new Address("4800 Lilly Ave, Edwards, CA", 793523), // Address.
            OrganizationType.NON_PROFIT, // Type.
            new Timestamp(System.currentTimeMillis()))); // Last update time.

        cache.putAll(data);
    }
}
