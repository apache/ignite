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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * This example demonstrates how to tweak particular settings of Apache Ignite page memory using
 * {@link MemoryConfiguration} and set up several memory policies for different caches with
 * {@link MemoryPolicyConfiguration}.
 * <p>
 * Additional remote nodes can be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} example-memory-policies.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which passing
 * {@code examples/config/example-memory-policies.xml} configuration to it.
 */
public class MemoryPoliciesExample {
    /** Name of the default memory policy defined in 'example-memory-policies.xml'. */
    public static final String POLICY_DEFAULT = "Default_Region";

    /** Name of the memory policy that creates a memory region limited by 20 MB with eviction enabled */
    public static final String POLICY_20MB_EVICTION = "20MB_Region_Eviction";

    /** Name of the memory policy that creates a memory region mapped to a memory-mapped file. */
    public static final String POLICY_15MB_MEMORY_MAPPED_FILE = "15MB_Region_Swapping";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-memory-policies.xml")) {
            System.out.println();
            System.out.println(">>> Memory policies example started.");

            /**
             * Preparing configurations for 2 caches that will be bound to the memory region defined by
             * '10MB_Region_Eviction' memory policy from 'example-memory-policies.xml' configuration.
             */
            CacheConfiguration<Integer, Integer> firstCacheCfg = new CacheConfiguration<>("firstCache");

            firstCacheCfg.setMemoryPolicyName(POLICY_20MB_EVICTION);
            firstCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            firstCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            CacheConfiguration<Integer, Integer> secondCacheCfg = new CacheConfiguration<>("secondCache");
            secondCacheCfg.setMemoryPolicyName(POLICY_20MB_EVICTION);
            secondCacheCfg.setCacheMode(CacheMode.REPLICATED);
            secondCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

            IgniteCache<Integer, Integer> firstCache = ignite.createCache(firstCacheCfg);
            IgniteCache<Integer, Integer> secondCache = ignite.createCache(secondCacheCfg);

            System.out.println(">>> Started two caches bound to '" + POLICY_20MB_EVICTION + "' memory region.");

            /**
             * Preparing a configuration for a cache that will be bound to the memory region defined by
             * '5MB_Region_Swapping' memory policy from 'example-memory-policies.xml' configuration.
             */
            CacheConfiguration<Integer, Integer> thirdCacheCfg = new CacheConfiguration<>("thirdCache");

            thirdCacheCfg.setMemoryPolicyName(POLICY_15MB_MEMORY_MAPPED_FILE);

            IgniteCache<Integer, Integer> thirdCache = ignite.createCache(thirdCacheCfg);

            System.out.println(">>> Started a cache bound to '" + POLICY_15MB_MEMORY_MAPPED_FILE + "' memory region.");


            /**
             * Preparing a configuration for a cache that will be bound to the default memory region defined by
             * default 'Default_Region' memory policy from 'example-memory-policies.xml' configuration.
             */
            CacheConfiguration<Integer, Integer> fourthCacheCfg = new CacheConfiguration<>("fourthCache");

            IgniteCache<Integer, Integer> fourthCache = ignite.createCache(fourthCacheCfg);

            System.out.println(">>> Started a cache bound to '" + POLICY_DEFAULT + "' memory region.");

            System.out.println(">>> Destroying caches...");

            firstCache.destroy();
            secondCache.destroy();
            thirdCache.destroy();
            fourthCache.destroy();
        }
    }
}