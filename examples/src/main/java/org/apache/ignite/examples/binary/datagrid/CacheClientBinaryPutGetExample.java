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

package org.apache.ignite.examples.binary.datagrid;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.OrganizationType;

/**
 * This example demonstrates use of binary objects with Ignite cache.
 * Specifically it shows that binary objects are simple Java POJOs and do not require any special treatment.
 * <p>
 * The example executes several put-get operations on Ignite cache with binary values. Note that
 * it demonstrates how binary object can be retrieved in fully-deserialized form or in binary object
 * format using special cache projection.
 * <p>
 * Remote nodes should always be started with the following command:
 * {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}
 * <p>
 * Alternatively you can run {@link org.apache.ignite.examples.ExampleNodeStartup} in another JVM which will
 * start a node with {@code examples/config/example-ignite.xml} configuration.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class CacheClientBinaryPutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheClientBinaryPutGetExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Binary objects cache put-get example started.");

            CacheConfiguration<Integer, Organization> cfg = new CacheConfiguration<>();

            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setName(CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

            try (IgniteCache<Integer, Organization> cache = ignite.getOrCreateCache(cfg)) {
                if (ignite.cluster().forDataNodes(cache.getName()).nodes().isEmpty()) {
                    System.out.println();
                    System.out.println(">>> This example requires remote cache node nodes to be started.");
                    System.out.println(">>> Please start at least 1 remote cache node.");
                    System.out.println(">>> Refer to example's javadoc for details on configuration.");
                    System.out.println();

                    return;
                }

                putGet(cache);
                putGetBinary(cache);
                putGetAll(cache);
                putGetAllBinary(cache);

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
        // Create new Organization binary object to store in cache.
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
     * Execute individual put and get, getting value in binary format, without de-serializing it.
     *
     * @param cache Cache.
     */
    private static void putGetBinary(IgniteCache<Integer, Organization> cache) {
        // Create new Organization binary object to store in cache.
        Organization org = new Organization(
            "Microsoft", // Name.
            new Address("1096 Eddy Street, San Francisco, CA", 94109), // Address.
            OrganizationType.PRIVATE, // Type.
            new Timestamp(System.currentTimeMillis())); // Last update time.

        // Put created data entry to cache.
        cache.put(1, org);

        // Get cache that will get values as binary objects.
        IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

        // Get recently created organization as a binary object.
        BinaryObject po = binaryCache.get(1);

        // Get organization's name from binary object (note that
        // object doesn't need to be fully deserialized).
        String name = po.field("name");

        System.out.println();
        System.out.println(">>> Retrieved organization name from binary object: " + name);
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations.
     *
     * @param cache Cache.
     */
    private static void putGetAll(IgniteCache<Integer, Organization> cache) {
        // Create new Organization binary objects to store in cache.
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
     * getting values in binary format, without de-serializing it.
     *
     * @param cache Cache.
     */
    private static void putGetAllBinary(IgniteCache<Integer, Organization> cache) {
        // Create new Organization binary objects to store in cache.
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

        // Get cache that will get values as binary objects.
        IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

        // Get recently created organizations as binary objects.
        Map<Integer, BinaryObject> poMap = binaryCache.getAll(map.keySet());

        Collection<String> names = new ArrayList<>();

        // Get organizations' names from binary objects (note that
        // objects don't need to be fully deserialized).
        for (BinaryObject po : poMap.values())
            names.add(po.<String>field("name"));

        System.out.println();
        System.out.println(">>> Retrieved organization names from binary objects: " + names);
    }
}
