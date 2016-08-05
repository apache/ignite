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

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Cache Affinity Put Get example.
 * <p>
 * Example demonstrates usage of affinity key for very basic operations on cache,
 * such as 'put' and 'get'.
 * <p>
 * Remote nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheAffinityPutGetExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /** Persons collocated with Organizations cache name. */
    private static final String COLLOCATED_PERSON_CACHE = CacheQueryExample.class.getSimpleName() + "CollocatedPersons";

    /** Persons cache name. */
    private static final String PERSON_CACHE = CacheQueryExample.class.getSimpleName() + "Persons";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> colPersonCacheCfg = new CacheConfiguration<>(COLLOCATED_PERSON_CACHE);

            colPersonCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            colPersonCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);

            CacheConfiguration<AffinityKey<Long>, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);

            personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            personCacheCfg.setIndexedTypes(Long.class, Person.class);

            // Auto-close cache at the end of the example.
            try (
                    IgniteCache<Long, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg);
                    IgniteCache<AffinityKey<Long>, Person> colPersonCache = ignite.getOrCreateCache(colPersonCacheCfg);
                    IgniteCache<AffinityKey<Long>, Person> personCache = ignite.getOrCreateCache(personCacheCfg)
            ) {
                // Populate cache.
                putGet();

            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(COLLOCATED_PERSON_CACHE);
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query example finished.");
        }
    }

    /**
     * Populate cache with test data.
     */
    private static void putGet() {
        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);

        // Clear cache before running the example.
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("ApacheIgnite");

        orgCache.put(org1.id(), org1);

        IgniteCache<AffinityKey<Long>, Person> colPersonCache = Ignition.ignite().cache(COLLOCATED_PERSON_CACHE);

        // Clear caches before running the example.
        colPersonCache.clear();

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        colPersonCache.put(p1.key(), p1);
        colPersonCache.put(p2.key(), p2);


        try {
            colPersonCache.get(new AffinityKey<>(p1.id));
        }catch (IgniteException e){
            X.printerr("Unable to get value p1 due to NULL affinity key.");
        }
        Person p11 = colPersonCache.get(p1.key());
        X.println("Got affinity key p1.key() with value "+p11);
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
