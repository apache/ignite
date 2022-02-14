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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class ConfiguringCaches {

    public static void main(String[] args) {
        configurationExample();
        cacheTemplateExample();
    }

    public static void configurationExample() {
        // tag::cfg[]
        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(2);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCacheConfiguration(cacheCfg);

        // Start a node.
        Ignition.start(cfg);
        // end::cfg[]
        Ignition.ignite().close();
    }

    public static void cacheTemplateExample() {
        // tag::template[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        try (Ignite ignite = Ignition.start(igniteCfg)) {
            CacheConfiguration cacheCfg = new CacheConfiguration("myCacheTemplate");

            cacheCfg.setBackups(2);
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);

            // Register the cache template 
            ignite.addCacheConfiguration(cacheCfg);
        }
        // end::template[]
    }

    static void backupsSync() {
        // tag::synchronization-mode[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        // Start the node.
        Ignition.start(cfg);
        // end::synchronization-mode[]
    }

    static void configuringBackups() {
        // tag::backups[]
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("cacheName");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);

        // end::backups[]
    }

}
