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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
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
 * The example executes several put-get operations on Ignite cache with portable values. Note that
 * it demonstrates how portable object can be retrieved in fully-deserialized form or in portable object
 * format using special cache projection.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables the portable marshaller: {@code 'ignite.{sh|bat} examples/config/portable/example-ignite-portable.xml'}.
 * <p>
 * Alternatively you can run {@link ExamplePortableNodeStartup} in another JVM which will
 * start node with {@code examples/config/portable/example-ignite-portable.xml} configuration.
 */
public class CacheClientPortablePutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheClientPortablePutGetExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/portable/example-ignite-portable.xml")) {
            System.out.println();
            System.out.println(">>> Portable objects cache put-get example started.");

            CacheConfiguration<Integer, Organization> cfg = new CacheConfiguration<>();

            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setName(CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

            try (IgniteCache<Integer, Organization> cache = ignite.createCache(cfg)) {
                if (ignite.cluster().forDataNodes(cache.getName()).nodes().isEmpty()) {
                    System.out.println();
                    System.out.println(">>> This example requires remote cache node nodes to be started.");
                    System.out.println(">>> Please start at least 1 remote cache node.");
                    System.out.println(">>> Refer to example's javadoc for details on configuration.");
                    System.out.println();

                    return;
                }

                putGet(cache);
                putGetPortable(cache);
                putGetAll(cache);
                putGetAllPortable(cache);

                System.out.println();
            }
            finally {
                // Delete cache with its content completely.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Execute individual put and get.
     *
     * @param cache Cache.
     */
    private static void putGet(IgniteCache<Integer, Organization> cache) {
        // Create new Organization portable object to store in cache.
        Organization org = new Organization(
            "Microsoft", // Name.
            new Address("1096 Eddy Street, San Francisco, CA", 94109), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        // Put created data entry to cache.
        cache.put(1, org);

        // Get recently created organization as a strongly-typed fully de-serialized instance.
        Organization orgFromCache = cache.get(1);

        System.out.println();
        System.out.println(">>> Retrieved organization instance from cache: " + orgFromCache);
    }

    /**
     * Execute individual put and get, getting value in portable format, without de-serializing it.
     *
     * @param cache Cache.
     */
    private static void putGetPortable(IgniteCache<Integer, Organization> cache) {
        // Create new Organization portable object to store in cache.
        Organization org = new Organization(
            "Microsoft", // Name.
            new Address("1096 Eddy Street, San Francisco, CA", 94109), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        // Put created data entry to cache.
        cache.put(1, org);

        // Get cache that will get values as portable objects.
        IgniteCache<Integer, PortableObject> portableCache = cache.withKeepPortable();

        // Get recently created organization as a portable object.
        PortableObject po = portableCache.get(1);

        // Get organization's name from portable object (note that
        // object doesn't need to be fully deserialized).
        String name = po.field("name");

        System.out.println();
        System.out.println(">>> Retrieved organization name from portable object: " + name);
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations.
     *
     * @param cache Cache.
     */
    private static void putGetAll(IgniteCache<Integer, Organization> cache) {
        // Create new Organization portable objects to store in cache.
        Organization org1 = new Organization(
            "Microsoft", // Name.
            new Address("1096 Eddy Street, San Francisco, CA", 94109), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        Organization org2 = new Organization(
            "Red Cross", // Name.
            new Address("184 Fidler Drive, San Antonio, TX", 78205), // Address.
            OrganizationType.NON_PROFIT, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        Map<Integer, Organization> map = new HashMap<>();

        map.put(1, org1);
        map.put(2, org2);

        // Put created data entries to cache.
        cache.putAll(map);

        // Get recently created organizations as a strongly-typed fully de-serialized instances.
        Map<Integer, Organization> mapFromCache = cache.getAll(map.keySet());

        System.out.println();
        System.out.println(">>> Retrieved organization instances from cache:");

        for (Organization org : mapFromCache.values())
            System.out.println(">>>     " + org);
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations,
     * getting values in portable format, without de-serializing it.
     *
     * @param cache Cache.
     */
    private static void putGetAllPortable(IgniteCache<Integer, Organization> cache) {
        // Create new Organization portable objects to store in cache.
        Organization org1 = new Organization(
            "Microsoft", // Name.
            new Address("1096 Eddy Street, San Francisco, CA", 94109), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        Organization org2 = new Organization(
            "Red Cross", // Name.
            new Address("184 Fidler Drive, San Antonio, TX", 78205), // Address.
            OrganizationType.NON_PROFIT, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        Map<Integer, Organization> map = new HashMap<>();

        map.put(1, org1);
        map.put(2, org2);

        // Put created data entries to cache.
        cache.putAll(map);

        // Get cache that will get values as portable objects.
        IgniteCache<Integer, PortableObject> portableCache = cache.withKeepPortable();

        // Get recently created organizations as portable objects.
        Map<Integer, PortableObject> poMap = portableCache.getAll(map.keySet());

        Collection<String> names = new ArrayList<>();

        // Get organizations' names from portable objects (note that
        // objects don't need to be fully deserialized).
        for (PortableObject po : poMap.values())
            names.add(po.<String>field("name"));

        System.out.println();
        System.out.println(">>> Retrieved organization names from portable objects: " + names);
    }
}
