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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_MODE;
import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests for WAL mode consistency validation when nodes join cluster. */
public class GridDiscoveryManagerWalModeConsistencyTest extends GridCommonAbstractTest {
    /** */
    private WALMode walMode;

    /** */
    private boolean mixedConfig;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setWalMode(walMode);

        if (mixedConfig) {
            storageCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("inmem")
                .setPersistenceEnabled(false));

            if (igniteInstanceName.contains("persistent_instance")) {
                DataRegionConfiguration persistentRegionCfg = new DataRegionConfiguration()
                    .setName("pds")
                    .setPersistenceEnabled(true);

                storageCfg.setDataRegionConfigurations(persistentRegionCfg);
            }
        }
        else
            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(walMode != null);

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
        walMode = LOG_ONLY;

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteEx cli = startClientGrid(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        cli.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

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
        walMode = LOG_ONLY;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        cli.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        walMode = FSYNC;

        String errMsg = assertThrowsWithCause(() -> startGrid(1), IgniteCheckedException.class).getCause().getMessage();

        checkErrMsg(errMsg, FSYNC, LOG_ONLY);

        assertEquals(2, ignite0.cluster().nodes().size());
    }

    /**
     * Tests that in-memory node can join cluster with persistence node with default WAL mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryNodeJoinsPersistenceNodeWithDefaultWalMode() throws Exception {
        doTestInMemoryNodeJoinsPersistenceNode(DFLT_WAL_MODE, null, true);
    }

    /**
     * Tests that in-memory node can join cluster with persistence node with FSYNC WAL mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryNodeJoinsPersistenceNodeWithFsyncWalMode() throws Exception {
        doTestInMemoryNodeJoinsPersistenceNode(FSYNC, null, true);
    }

    /**
     * Tests that persistence node with default WAL mode can join cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceNodeWithDefaultWalModeJoinsSuccessfully() throws Exception {
        doTestInMemoryNodeJoinsPersistenceNode(DFLT_WAL_MODE, DFLT_WAL_MODE, false);
    }

    /**
     * Tests that two in-memory nodes can join cluster successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryNodesJoinSuccessfully() throws Exception {
        doTestInMemoryNodeJoinsPersistenceNode(null, null, false);
    }

    /**
     * Tests mixed cluster: cluster is in-memory with null walMode, node with
     * in-memory + persistent regions joins succesfully, walModes are the same
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedClusterInMemoryJoinsPersistentWithDefaultWalMode() throws Exception {
        IgniteEx inMemoryNode = doCreateInMemoryClusterWithMixedConfig();

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
        IgniteEx inMemoryNode = doCreateInMemoryClusterWithMixedConfig();

        walMode = FSYNC;

        Throwable err = assertThrowsWithCause(() -> startGrid("persistent_instance"), IgniteCheckedException.class);

        checkErrMsg(err.getCause().getMessage(), FSYNC, LOG_ONLY);

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
        IgniteEx inMemoryNode = doCreateInMemoryClusterWithMixedConfig();

        walMode = DFLT_WAL_MODE;

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
        walMode = FSYNC;

        IgniteEx persistentNode0 = startGrid("persistent_instance0");

        persistentNode0.cluster().state(ClusterState.ACTIVE);

        IgniteEx cli = startClientGrid("client");

        cli.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        walMode = DFLT_WAL_MODE;

        Throwable err = assertThrowsWithCause(() -> startGrid("persistent_instance1"), IgniteCheckedException.class);

        checkErrMsg(err.getCause().getMessage(), DFLT_WAL_MODE, FSYNC);

        assertEquals(2, persistentNode0.cluster().nodes().size());
    }

    /**
     * Creates cluster with given walMode and expect in-memory node can't join.
     *
     * @param wal1 walMode to use in cluster.
     * @param wal1 walMode to use for joining node.
     * @throws Exception If failed.
     */
    private void doTestInMemoryNodeJoinsPersistenceNode(WALMode wal1, WALMode wal2, boolean shouldFail) throws Exception {
        walMode = wal1;

        IgniteEx ignite0 = startGrid(0);

        IgniteEx cli = startClientGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheCli.put(1, 1);

        walMode = wal2;

        if (shouldFail) {
            assertThrowsWithCause(() -> startGrid(2), IgniteCheckedException.class);

            assertEquals(2, ignite0.cluster().nodes().size());
        }
        else {
            IgniteEx ignite1 = startGrid(2);

            assertEquals(3, ignite0.cluster().nodes().size());
            assertEquals(3, ignite1.cluster().nodes().size());
        }
    }

    /**
     * Creates in-memory cluster with node "in-memory_instance" with mixed config.
     *
     * @return IgniteEx inMemoryNode with access to cluster.
     * @throws Exception If failed.
     */
    private IgniteEx doCreateInMemoryClusterWithMixedConfig() throws Exception {
        mixedConfig = true;
        walMode = null;

        IgniteEx inMemoryNode = startGrid("in-memory_instance");

        IgniteEx cli = startClientGrid("client");

        inMemoryNode.cluster().baselineAutoAdjustEnabled(false);

        inMemoryNode.cluster().state(ClusterState.ACTIVE);

        cli.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        return inMemoryNode;
    }

    /** */
    private static void checkErrMsg(String msg, WALMode loc, WALMode rmt) {
        assertTrue(msg.startsWith("Remote node has WAL mode different from local"));
        assertTrue(msg.contains("locWalMode=" + loc));
        assertTrue(msg.contains("rmtWalMode=" + rmt));
    }
}
