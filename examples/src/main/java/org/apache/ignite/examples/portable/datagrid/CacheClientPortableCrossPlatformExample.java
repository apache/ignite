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

package org.apache.ignite.examples.portable.datagrid;

import java.sql.Timestamp;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.portable.Address;
import org.apache.ignite.examples.portable.ExamplePortableNodeStartup;
import org.apache.ignite.examples.portable.Organization;
import org.apache.ignite.examples.portable.OrganizationType;
import org.apache.ignite.portable.PortableObject;

/**
 * This example demonstrates use of portable objects with Ignite cache.
 * Specifically it shows that portable objects are simple Java POJOs and do not require any special treatment.
 * <p>
 * The example creates and puts an entry to cache. It tries to get entries by keys that are used by similar examples
 * for .NET client. So if you run, for example, .NET example and then this example without restarting
 * the data node, you will receive data stored in cache by .NET client.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables the portable marshaller: {@code 'ignite.{sh|bat} examples/config/portable/example-ignite-portable.xml'}.
 * <p>
 * Alternatively you can run {@link ExamplePortableNodeStartup} in another JVM which will
 * start node with {@code examples/config/portable/example-ignite-portable.xml} configuration.
 */
public class CacheClientPortableCrossPlatformExample {
    /** Java client key. */
    private static final int JAVA_KEY = 100;

    /** .NET client key. */
    private static final int DOT_NET_KEY = 200;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/portable/example-ignite-portable.xml")) {
            System.out.println();
            System.out.println(">>> Portable objects cross-platform example started.");

            CacheConfiguration<Integer, Organization> cfg = new CacheConfiguration<>();

            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cfg.setBackups(1);
            cfg.setName("cacheCrossPlatform");

            try (IgniteCache<Integer, Organization> cache = ignite.getOrCreateCache(cfg)) {
                if (ignite.cluster().forDataNodes(cache.getName()).nodes().isEmpty()) {
                    System.out.println();
                    System.out.println(">>> This example requires remote cache nodes to be started.");
                    System.out.println(">>> Please start at least 1 remote cache node.");
                    System.out.println(">>> Refer to example's javadoc for details on configuration.");
                    System.out.println();

                    return;
                }

                // Create new Organization portable object to store in cache.
                Organization org = new Organization(
                    "Microsoft", // Name.
                    new Address("123 Portable Street, San Francisco, CA", 94103), // Address.
                    OrganizationType.PRIVATE, // Type.
                    new Timestamp(System.currentTimeMillis())); // Last update time.

                // Put created data entry to cache.
                cache.put(JAVA_KEY, org);

                // Retrieve value stored by .NET client.
                getFromDotNet(cache);

                // Gets portable value from cache in portable format, without de-serializing it.
                getJavaPortableInstance(cache);

                // Gets portable value form cache as a strongly-typed fully de-serialized instance.
                getJavaTypedInstance(cache);

                System.out.println();
            }
        }
    }

    /**
     * Gets entry put by .NET client. In order for entry to be in cache, .NET client example
     * must be run before this example.
     *
     * @param cache Cache.
     */
    private static void getFromDotNet(IgniteCache<Integer, Organization> cache) {
        // Get entry that was put by .NET client.
        Organization org = cache.get(DOT_NET_KEY);

        System.out.println();

        if (org == null)
            System.out.println(">>> Entry from .NET client not found (run .NET example before this example)");
        else {
            System.out.println(">>> Entry from .NET client:");
            System.out.println(">>>     " + org);
        }
    }

    /**
     * Gets portable value from cache in portable format, without de-serializing it.
     *
     * @param cache Cache.
     */
    private static void getJavaPortableInstance(IgniteCache<Integer, Organization> cache) {
        // Gets cache that will get values as portable objects.
        IgniteCache<Integer, PortableObject> portableCache = cache.withKeepPortable();

        // Gets recently created employee as a portable object.
        PortableObject po = portableCache.get(JAVA_KEY);

        assert po != null;

        // Gets employee's name from portable object (note that
        // object doesn't need to be fully deserialized).
        String name = po.field("name");

        System.out.println();
        System.out.println(">>> Retrieved organization name from portable field: " + name);
    }

    /**
     * Gets portable value form cache as a strongly-typed fully de-serialized instance.
     *
     * @param cache Cache name.
     */
    private static void getJavaTypedInstance(IgniteCache<Integer, Organization> cache) {
        // Get recently created employee as a strongly-typed fully de-serialized instance.
        Organization org = cache.get(JAVA_KEY);

        assert org != null;

        String name = org.name();

        System.out.println();
        System.out.println(">>> Retrieved organization name from deserialized Organization instance: " + name);
    }
}