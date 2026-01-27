/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests for WAL mode consistency validation when nodes join cluster. */
public class GridDiscoveryManagerWalModeConsistencyTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSISTENT_REGION_NAME = "pds-reg";

    /** */
    private WALMode walMode;

    /** */
    private boolean mixedConfig;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        if (mixedConfig) {
            storageCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().setName("default_in_memory_region")
                .setPersistenceEnabled(false));

            if (igniteInstanceName.contains("persistent_instance")) {
                DataRegionConfiguration persistentRegionCfg = new DataRegionConfiguration().setName(PERSISTENT_REGION_NAME)
                    .setPersistenceEnabled(true);

                if (walMode != null)
                    storageCfg.setWalMode(walMode);

                storageCfg.setDataRegionConfigurations(persistentRegionCfg);
            }
        }
        else {
            if (walMode != null) {
                storageCfg.setWalMode(walMode);
                storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
            }
            else {
                storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(false);
            }
        }

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        walMode = null;
        mixedConfig = false;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Tests that nodes with same WAL mode can join cluster successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSameWalModeJoinsSuccessfully() throws Exception {
        walMode = WALMode.LOG_ONLY;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx ignite1 = startGrid(1);

        IgniteEx cli = startClientGrid(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        assertEquals(3, ignite0.cluster().nodes().size());
        assertEquals(3, ignite1.cluster().nodes().size());
    }

    /**
     * Tests that nodes with different WAL modes cannot join cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDifferentWalModesCannotJoin() throws Exception {
        walMode = WALMode.LOG_ONLY;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = WALMode.FSYNC;

        String errMsg = GridTestUtils.assertThrowsWithCause(() -> startGrid(1), IgniteCheckedException.class)
            .getCause().getMessage();

        assertTrue(errMsg.startsWith("Remote node has WAL mode different from local") &&
            (errMsg.contains("locWalMode=FSYNC") && errMsg.contains("rmtWalMode=LOG_ONLY")) ||
            (errMsg.contains("locWalMode=LOG_ONLY") && errMsg.contains("rmtWalMode=FSYNC")));

        assertEquals(2, ignite0.cluster().nodes().size());
    }

    /**
     * Tests that in-memory node can join cluster with persistence node with default WAL mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryNodeJoinsPersistenceNodeWithDefaultWalMode() throws Exception {
        walMode = DataStorageConfiguration.DFLT_WAL_MODE;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = null;
        GridTestUtils.assertThrowsWithCause(() -> startGrid(2), IgniteCheckedException.class);

        assertEquals(2, ignite0.cluster().nodes().size());
    }

    /**
     * Tests that in-memory node can join cluster with persistence node with FSYNC WAL mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryNodeJoinsPersistenceNodeWithFsyncWalMode() throws Exception {
        walMode = WALMode.FSYNC;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = null;
        GridTestUtils.assertThrowsWithCause(() -> startGrid(2), IgniteCheckedException.class);

        assertEquals(2, ignite0.cluster().nodes().size());
    }

    /**
     * Tests that persistence node with default WAL mode can join cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceNodeWithDefaultWalModeJoinsSuccessfully() throws Exception {
        walMode = DataStorageConfiguration.DFLT_WAL_MODE;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = DataStorageConfiguration.DFLT_WAL_MODE;
        IgniteEx ignite1 = startGrid(1);

        assertEquals(3, ignite0.cluster().nodes().size());
        assertEquals(3, ignite1.cluster().nodes().size());
    }

    /**
     * Tests that two in-memory nodes can join cluster successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryNodesJoinSuccessfully() throws Exception {
        walMode = null;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = null;
        IgniteEx ignite1 = startGrid(1);

        assertEquals(3, ignite0.cluster().nodes().size());
        assertEquals(3, ignite1.cluster().nodes().size());
    }

    /**
     * Tests mixed cluster: cluster is in-memory with null walMode, node with
     * in-memory + persistent regions joins succesfully, walModes are the same
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedClusterInMemoryJoinsPersistentWithDefaultWalMode() throws Exception {
        mixedConfig = true;
        walMode = null;

        IgniteEx inMemoryNode = startGrid("in-memory_instance");

        IgniteEx cli = startClientGrid("client");

        inMemoryNode.cluster().baselineAutoAdjustEnabled(false);

        inMemoryNode.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        startGrid("persistent_instance");

        assertEquals(3, inMemoryNode.cluster().nodes().size());
    }

    /**
     * Tests mixed cluster: in-memory node cluster, persistent node joins
     * (has default in-memory region + persistent region with FSYNC WAL mode).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedClusterInMemoryJoinsPersistentWithFsyncWalMode() throws Exception {
        mixedConfig = true;
        walMode = null;

        IgniteEx inMemoryNode = startGrid("in-memory_instance");

        IgniteEx cli = startClientGrid("client");

        inMemoryNode.cluster().baselineAutoAdjustEnabled(false);

        inMemoryNode.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = WALMode.FSYNC;
        String errMsg = GridTestUtils.assertThrowsWithCause(() -> startGrid("persistent_instance"), IgniteCheckedException.class)
            .getCause().getMessage();

        assertTrue(errMsg.startsWith("Remote node has WAL mode different from local") &&
            (errMsg.contains("locWalMode=FSYNC") && errMsg.contains("rmtWalMode=LOG_ONLY")) ||
            (errMsg.contains("locWalMode=LOG_ONLY") && errMsg.contains("rmtWalMode=FSYNC")));

        assertEquals(2, inMemoryNode.cluster().nodes().size());
    }

    /**
     * Tests mixed cluster: in-memory node cluster, persistent node joins
     * (has default in-memory region + persistent region with Default WAL mode).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedClusterInMemoryJoinsPersistentWithDfltWalMode() throws Exception {
        mixedConfig = true;
        walMode = null;

        IgniteEx inMemoryNode = startGrid("in-memory_instance");

        IgniteEx cli = startClientGrid("client");

        inMemoryNode.cluster().baselineAutoAdjustEnabled(false);

        inMemoryNode.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = DataStorageConfiguration.DFLT_WAL_MODE;
        startGrid("persistent_instance");

        assertEquals(3, inMemoryNode.cluster().nodes().size());
    }

    /**
     * Tests mixed cluster: persistent node with default WAL mode cannot join
     * cluster with persistent node with FSYNC WAL mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedClusterDifferentWalModesCannotJoin() throws Exception {
        walMode = WALMode.FSYNC;

        IgniteEx persistentNode0 = startGrid("persistent_instance0");

        persistentNode0.cluster().state(ClusterState.ACTIVE);

        IgniteEx cli = startClientGrid("client");

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = DataStorageConfiguration.DFLT_WAL_MODE;

        String errMsg = GridTestUtils.assertThrowsWithCause(() -> startGrid("persistent_instance1"), IgniteCheckedException.class)
            .getCause().getMessage();

        assertTrue(errMsg.startsWith("Remote node has WAL mode different from local") &&
                (errMsg.contains("locWalMode=FSYNC") && errMsg.contains("rmtWalMode=" + DataStorageConfiguration.DFLT_WAL_MODE.name())) ||
                (errMsg.contains("locWalMode=" + DataStorageConfiguration.DFLT_WAL_MODE.name()) && errMsg.contains("rmtWalMode=FSYNC")));

        assertEquals(2, persistentNode0.cluster().nodes().size());
    }
}