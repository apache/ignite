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

package org.apache.ignite.compatibility.upgrade;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Smoke test for rolling upgrade with persistence.
 */
@RunWith(Parameterized.class)
public class IgniteRebalanceOnUpgradeTest extends IgniteUpgradeAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "transactional-cache";

    /** */
    @Parameterized.Parameter
    public boolean persistentEnabled;

    /** */
    @Parameterized.Parameters(name = "persistentEnabled={0}")
    public static Object[] params() {
        return new Object[] {false, true};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistentEnabled)
                .setMaxSize(300 * 1024 * 1024)));

        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /** */
    @Test
    public void testRollingUpgrade() throws Exception {
        IgniteEx ign = startBaseCluster(3);

        ign.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ign.cache(CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        upgradeNode(1);
        upgradeNode(2);
        ign = upgradeNode(3);

        IgniteCache<Integer, Integer> targetCache = ign.cache(CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            assertEquals("Data mismatch after upgrade at key: " + i, i, (int)targetCache.get(i));

        targetCache.put(1001, 1001);

        assertEquals(1001, (int)targetCache.get(1001));
    }

    /** */
    private CacheConfiguration<Integer, Integer> getCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }
}
