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

package org.apache.ignite.internal.encryption;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 */
public class EncryptedCacheNodeJoinTest extends AbstractEncryptionTest {
    /** */
    private static final String GRID_2 = "grid-2";

    /** */
    private static final String GRID_3 = "grid-3";

    /** */
    private static final String GRID_4 = "grid-4";

    /** */
    private static final String GRID_5 = "grid-5";

    /** */
    private static final String GRID_6 = "grid-6";

    /** */
    private static final String GRID_7 = "grid-7";

    /** */
    public static final String CLIENT = "client";

    /** */
    private boolean configureCache;

    /** */
    private static final String KEYSTORE_PATH_2 =
        IgniteUtils.resolveIgnitePath("modules/core/src/test/resources/other_tde_keystore.jks").getAbsolutePath();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        configureCache = false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String grid) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(grid);

        cfg.setConsistentId(grid);

        if (grid.equals(GRID_0) ||
            grid.equals(GRID_2) ||
            grid.equals(GRID_3) ||
            grid.equals(GRID_4) ||
            grid.equals(GRID_5) ||
            grid.equals(GRID_6) ||
            grid.equals(GRID_7)) {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath(grid.equals(GRID_2) ? KEYSTORE_PATH_2 : KEYSTORE_PATH);
            encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

            cfg.setEncryptionSpi(encSpi);
        }
        else
            cfg.setEncryptionSpi(null);

        if (configureCache)
            cfg.setCacheConfiguration(cacheConfiguration(grid));

        return cfg;
    }

    /** */
    protected CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName());

        if (gridName.startsWith(CLIENT) ||
            gridName.equals(GRID_0) ||
            gridName.equals(GRID_6) ||
            gridName.equals(GRID_7))
            ccfg.setEncryptionEnabled(true);

        return ccfg;
    }

    /** */
    @Test
    public void testNodeCantJoinWithoutEncryptionSpi() throws Exception {
        startGrid(GRID_0);

        assertThrowsWithCause(() -> {
            try {
                startGrid(GRID_1);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testNodeCantJoinWithDifferentKeyStore() throws Exception {
        startGrid(GRID_0);

        assertThrowsWithCause(() -> {
            try {
                startGrid(GRID_2);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testNodeCanJoin() throws Exception {
        startGrid(GRID_0);

        startGrid(GRID_3).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testNodeCantJoinWithDifferentCacheKeys() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);
        startGrid(GRID_3);

        grid0.cluster().state(ClusterState.ACTIVE);

        stopGrid(GRID_3, false);

        createEncryptedCache(grid0, null, cacheName(), null, false);

        stopGrid(GRID_0, false);
        IgniteEx grid3 = startGrid(GRID_3);

        grid3.cluster().state(ClusterState.ACTIVE);

        createEncryptedCache(grid3, null, cacheName(), null, false);

        assertThrowsWithCause(() -> {
            try {
                startGrid(GRID_0);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testThirdNodeCanJoin() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);

        IgniteEx grid3 = startGrid(GRID_3);

        grid3.cluster().state(ClusterState.ACTIVE);

        createEncryptedCache(grid0, grid3, cacheName(), null);

        checkEncryptedCaches(grid0, grid3);

        IgniteEx grid4 = startGrid(GRID_4);

        awaitPartitionMapExchange();

        checkEncryptedCaches(grid0, grid4);
    }

    /** */
    @Test
    public void testClientNodeJoin() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);

        IgniteEx grid3 = startGrid(GRID_3);

        grid3.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid(CLIENT);

        createEncryptedCache(client, grid0, cacheName(), null);
    }

    /** */
    @Test
    public void testClientNodeJoinActiveClusterWithNewStaticCacheConfig() throws Exception {
        checkNodeJoinWithStaticCacheConfig(true, true, true);
    }

    /** */
    @Test
    public void testClientNodeJoinActiveClusterWithExistingStaticCacheConfig() throws Exception {
        checkNodeJoinWithStaticCacheConfig(true, true, false);
    }

    /** */
    @Test
    public void testClientNodeJoinInactiveClusterWithNewStaticCacheConfig() throws Exception {
        checkNodeJoinWithStaticCacheConfig(true, false, true);
    }

    /** */
    @Test
    public void testClientNodeJoinInactiveClusterWithExistingStaticCacheConfig() throws Exception {
        checkNodeJoinWithStaticCacheConfig(true, false, false);
    }

    /** */
    @Test
    public void testServerNodeJoinActiveClusterWithNewStaticCacheConfig() throws Exception {
        checkNodeJoinWithStaticCacheConfig(false, true, true);
    }

    /** */
    @Test
    public void testServerNodeJoinInactiveClusterWithNewStaticCacheConfig() throws Exception {
        checkNodeJoinWithStaticCacheConfig(false, false, true);
    }

    /**
     * @param client {@code True} to test client node join, {@code False} to test server node join.
     * @param activateBeforeJoin {@code True} to activate the server before joining the client node.
     * @param newCfg {@code True} to configure cache on the last joined node. {@code False} to configure on all nodes.
     */
    private void checkNodeJoinWithStaticCacheConfig(
        boolean client,
        boolean activateBeforeJoin,
        boolean newCfg
    ) throws Exception {
        if (!newCfg)
            configureCache = true;

        startGrid(GRID_0);
        startGrid(GRID_6);

        IgniteEx client1 = startClientGrid("client1");

        if (newCfg)
            configureCache = true;

        if (activateBeforeJoin)
            grid(GRID_0).cluster().state(ClusterState.ACTIVE);

        if (client && newCfg) {
            String expErrMsg = "Joining node has encrypted caches which are not presented on the cluster, " +
                "encrypted caches configured on client node cannot be started when such node joins " +
                "the cluster, these caches can be started manually (dynamically) after node joined" +
                "[caches=" + cacheName() + ']';

            GridTestUtils.assertThrowsAnyCause(log, () -> startClientGrid(CLIENT), IgniteSpiException.class, expErrMsg);

            return;
        }

        IgniteEx node = client ? startClientGrid(CLIENT) : startGrid(GRID_7);

        if (!activateBeforeJoin)
            grid(GRID_0).cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = node.cache(cacheName());

        assertNotNull(cache);

        for (long i = 0; i < 100; i++)
            cache.put(i, String.valueOf(i));

        checkEncryptedCaches(grid(GRID_0), grid(GRID_6));
        checkEncryptedCaches(grid(GRID_0), client1);
        checkData(client1);

        if (client) {
            checkEncryptedCaches(grid(GRID_0), grid(CLIENT));
            checkData(grid(CLIENT));
        }
        else
            checkEncryptedCaches(grid(GRID_7), grid(GRID_0));
    }

    /** */
    @Test
    public void testNodeCantJoinWithSameNameButNotEncCache() throws Exception {
        configureCache = true;

        IgniteEx grid0 = startGrid(GRID_0);

        grid0.cluster().state(ClusterState.ACTIVE);

        assertThrowsWithCause(() -> {
            try {
                startGrid(GRID_5);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testNodeCantJoinWithSameNameButEncCache() throws Exception {
        configureCache = true;

        IgniteEx grid0 = startGrid(GRID_5);

        grid0.cluster().state(ClusterState.ACTIVE);

        assertThrowsWithCause(() -> {
            try {
                startGrid(GRID_0);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, IgniteCheckedException.class);
    }
}
