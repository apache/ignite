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
package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheConfigurationChecksOnNodeJoinTest extends GridCommonAbstractTest {

    private static final String TEST_CACHE_NAME = "cache123";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

        if (dsCfg == null)
            dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    public void test() throws Exception {
        startGrids(3);

        Ignite node0 = grid(0);

        node0.cluster().active(true);

        stopGrid(2);

        node0.cluster().setBaselineTopology(node0.cluster().topologyVersion());

        CacheConfiguration<Integer, Integer> cacheCfg =
            new CacheConfiguration<Integer, Integer>(TEST_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0);

        try (IgniteCache<Integer, Integer> cache = node0.getOrCreateCache(cacheCfg)) {
            IntStream.range(0, 10).forEach(i -> cache.put(i, i));
        }

        stopGrid(0);

        IgniteEx node1 = grid(1);

        try(IgniteCache<Integer, Integer> cache = node1.getOrCreateCache(cacheCfg)){
            Affinity<Object> affinity = node1.affinity(TEST_CACHE_NAME);

            long lost = IntStream.range(0,10)
                .filter(i->!affinity.isPrimaryOrBackup(node1.localNode(), i) || !cache.containsKey(i))
                .count();

            log.info("Lost keys count: " + lost);

            assertNotSame("Some keys must be lost!", 0, lost);
        }

        stopAllGrids();

        startGrids(3);

        Ignite node2 = grid(2);

        node2.cluster().active(true);

    }
}