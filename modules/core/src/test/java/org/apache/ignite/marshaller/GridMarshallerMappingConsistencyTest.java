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

package org.apache.ignite.marshaller;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridMarshallerMappingConsistencyTest extends GridCommonAbstractTest {
    /** Test cache name. */
    private static final String CACHE_NAME = "cache";

    /** Work directory. */
    private static final String WORK_DIR = U.getIgniteHome() +
        File.separatorChar + "work" +
        File.separatorChar + "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setConsistentId(igniteInstanceName);

        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setPersistenceEnabled(true).setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(drCfg);

        igniteCfg.setDataStorageConfiguration(dsCfg);

        igniteCfg.setWorkDirectory(WORK_DIR + File.separator + igniteInstanceName);

        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearWorkDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        clearWorkDir();
    }

    /**
     * @throws IOException If IO error happens.
     */
    private void clearWorkDir() throws IOException {
        FileUtils.deleteDirectory(new File(WORK_DIR));
    }

    /**
     * Make a value be rebalanced to a node after the mapping was accepted.
     * Check, that the mapping is available after restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMappingsPersistedOnJoin() throws Exception {
        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        g1.cluster().active(true);

        CacheConfiguration<Integer, DummyObject> cacheCfg = new CacheConfiguration<>(CACHE_NAME);
        cacheCfg.setBackups(1);

        IgniteCache<Integer, DummyObject> c1 = g1.getOrCreateCache(cacheCfg);
        IgniteCache<Integer, DummyObject> c2 = g2.getOrCreateCache(cacheCfg);

        int k = primaryKey(c2);

        stopGrid(2);

        c1.put(k, new DummyObject(k));

        startGrid(2);

        awaitPartitionMapExchange();

        stopAllGrids();

        g2 = startGrid(2);

        g2.cluster().active(true);

        c2 = g2.cache(CACHE_NAME);

        assertEquals(k, c2.get(k).val);
    }

    /**
     * Connect a node to a cluster, that already has persisted data and a mapping.
     * Check, that persisted mappings are distributed to existing nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistedMappingsSharedOnJoin() throws Exception {
        Ignite g1 = startGrid(1);
        startGrid(2);

        g1.cluster().active(true); // Include second node into baseline topology.

        stopGrid(2);

        IgniteCache<Integer, DummyObject> c1 = g1.getOrCreateCache(CACHE_NAME);

        int k = primaryKey(c1);

        c1.put(k, new DummyObject(k));

        stopAllGrids();

        startGrid(1);
        Ignite g2 = startGrid(2);

        assertTrue("Failed to wait for automatic grid activation",
            GridTestUtils.waitForCondition(() -> g2.cluster().active(), getTestTimeout()));

        IgniteCache<Integer, DummyObject> c2 = g2.cache(CACHE_NAME);

        assertEquals(k, c2.get(k).val);
    }

    /**
     *
     */
    private static class DummyObject {
        /** */
        int val;

        /**
         * @param val Value.
         */
        DummyObject(int val) {
            this.val = val;
        }
    }
}
