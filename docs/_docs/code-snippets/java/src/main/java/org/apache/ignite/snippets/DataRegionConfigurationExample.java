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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class DataRegionConfigurationExample {

    public static void main(String[] args) {

        //tag::ignite-config[]
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        //tag::default[]

        DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
        defaultRegion.setName("Default_Region");
        defaultRegion.setInitialSize(100 * 1024 * 1024);

        storageCfg.setDefaultDataRegionConfiguration(defaultRegion);
        //end::default[]
        //tag::data-regions[]
        // 40MB memory region with eviction enabled.
        DataRegionConfiguration regionWithEviction = new DataRegionConfiguration();
        regionWithEviction.setName("40MB_Region_Eviction");
        regionWithEviction.setInitialSize(20 * 1024 * 1024);
        regionWithEviction.setMaxSize(40 * 1024 * 1024);
        regionWithEviction.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);

        storageCfg.setDataRegionConfigurations(regionWithEviction);
        //end::data-regions[]

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDataStorageConfiguration(storageCfg);
        //tag::caches[]

        CacheConfiguration cache1 = new CacheConfiguration("SampleCache");
        //this cache will be hosted in the "40MB_Region_Eviction" data region
        cache1.setDataRegionName("40MB_Region_Eviction");

        cfg.setCacheConfiguration(cache1);
        //end::caches[]

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::ignite-config[]

        ignite.close();
    }
}
