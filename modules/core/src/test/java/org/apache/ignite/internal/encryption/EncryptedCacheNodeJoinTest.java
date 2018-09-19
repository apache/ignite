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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;

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
            grid.equals(GRID_5)) {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath(grid.equals(GRID_2) ? KEYSTORE_PATH_2 : KEYSTORE_PATH);
            encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

            cfg.setEncryptionSpi(encSpi);
        }
        else
            cfg.setEncryptionSpi(null);

        cfg.setClientMode(grid.equals(CLIENT));

        if (configureCache)
            cfg.setCacheConfiguration(cacheConfiguration(grid));

        return cfg;
    }

    /** */
    protected CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName());
        ccfg.setEncryptionEnabled(gridName.equals(GRID_0));

        return ccfg;
    }

    /** */
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
    public void testNodeCanJoin() throws Exception {
        startGrid(GRID_0);

        startGrid(GRID_3).cluster().active(true);
    }

    /** */
    public void testNodeCantJoinWithDifferentCacheKeys() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);
        startGrid(GRID_3);

        grid0.cluster().active(true);

        stopGrid(GRID_3, false);

        createEncryptedCache(grid0, null, cacheName(), null, false);

        stopGrid(GRID_0, false);
        IgniteEx grid3 = startGrid(GRID_3);

        grid3.cluster().active(true);

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
    public void testThirdNodeCanJoin() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);

        IgniteEx grid3 = startGrid(GRID_3);

        grid3.cluster().active(true);

        createEncryptedCache(grid0, grid3, cacheName(), null);

        checkEncryptedCaches(grid0, grid3);

        IgniteEx grid4 = startGrid(GRID_4);

        awaitPartitionMapExchange();

        checkEncryptedCaches(grid0, grid4);
    }

    /** */
    public void testClientNodeJoin() throws Exception {
        IgniteEx grid0 = startGrid(GRID_0);

        IgniteEx grid3 = startGrid(GRID_3);

        grid3.cluster().active(true);

        IgniteEx client = startGrid(CLIENT);

        createEncryptedCache(client, grid0, cacheName(), null);
    }

    /** */
    public void testNodeCantJoinWithSameNameButNotEncCache() throws Exception {
        configureCache = true;

        IgniteEx grid0 = startGrid(GRID_0);

        grid0.cluster().active(true);

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
    public void testNodeCantJoinWithSameNameButEncCache() throws Exception {
        configureCache = true;

        IgniteEx grid0 = startGrid(GRID_5);

        grid0.cluster().active(true);

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
