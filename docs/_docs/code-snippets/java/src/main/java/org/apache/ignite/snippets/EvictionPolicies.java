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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class EvictionPolicies {

    public static void runAll() {
        randomLRU();
        random2LRU();
        LRU();
        FIFO();
        sorted();
    }

    public static void randomLRU() {
        //tag::randomLRU[]
        // Node configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Memory configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Creating a new data region.
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("20GB_Region");

        // 500 MB initial size (RAM).
        regionCfg.setInitialSize(500L * 1024 * 1024);

        // 20 GB max size (RAM).
        regionCfg.setMaxSize(20L * 1024 * 1024 * 1024);

        // Enabling RANDOM_LRU eviction for this region.
        regionCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);

        // Setting the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);

        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);
        //end::randomLRU[]

        try (Ignite ignite = Ignition.start(new IgniteConfiguration().setDataStorageConfiguration(storageCfg))) {

        }
    }

    public static void random2LRU() {
        //tag::random2LRU[]
        // Ignite configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Memory configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Creating a new data region.
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("20GB_Region");

        // 500 MB initial size (RAM).
        regionCfg.setInitialSize(500L * 1024 * 1024);

        // 20 GB max size (RAM).
        regionCfg.setMaxSize(20L * 1024 * 1024 * 1024);

        // Enabling RANDOM_2_LRU eviction for this region.
        regionCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);

        // Setting the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);

        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);
        //end::random2LRU[]

    }

    public static void LRU() {
        //tag::LRU[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");

        // Enabling on-heap caching for this distributed cache.
        cacheCfg.setOnheapCacheEnabled(true);

        // Set the maximum cache size to 1 million (default is 100,000).
        cacheCfg.setEvictionPolicyFactory(() -> new LruEvictionPolicy(1000000));

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);
        //end::LRU[]

    }

    public static void FIFO() {
        //tag::FIFO[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");

        // Enabling on-heap caching for this distributed cache.
        cacheCfg.setOnheapCacheEnabled(true);

        // Set the maximum cache size to 1 million (default is 100,000).
        cacheCfg.setEvictionPolicyFactory(() -> new FifoEvictionPolicy(1000000));

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);
        //end::FIFO[]

        
    }

    public static void sorted() {
        //tag::sorted[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");

        // Enabling on-heap caching for this distributed cache.
        cacheCfg.setOnheapCacheEnabled(true);

        // Set the maximum cache size to 1 million (default is 100,000).
        cacheCfg.setEvictionPolicyFactory(() -> new SortedEvictionPolicy(1000000));

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);
        //end::sorted[]

    }
}
