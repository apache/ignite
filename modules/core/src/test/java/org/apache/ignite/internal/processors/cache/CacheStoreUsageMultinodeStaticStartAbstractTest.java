/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.junit.Test;

/**
 *
 */
public abstract class CacheStoreUsageMultinodeStaticStartAbstractTest extends CacheStoreUsageMultinodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfiguration() throws Exception {
        checkStoreUpdateStaticConfig(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationNearEnabled() throws Exception {
        nearCache = true;

        checkStoreUpdateStaticConfig(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationTxWriteBehindStore() throws Exception {
        writeBehind = true;

        checkStoreUpdateStaticConfig(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationTxWriteBehindStoreNearEnabled() throws Exception {
        writeBehind = true;

        nearCache = true;

        checkStoreUpdateStaticConfig(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationTxWriteBehindStoreNoClientStore() throws Exception {
        writeBehind = true;

        checkStoreUpdateStaticConfig(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationTxWriteBehindStoreNoClientStoreNearEnabled() throws Exception {
        writeBehind = true;

        nearCache = true;

        checkStoreUpdateStaticConfig(false);
    }

    /**
     * @param clientStore {@code True} if store should be configured on client node.
     * @throws Exception If failed.
     */
    private void checkStoreUpdateStaticConfig(boolean clientStore) throws Exception {
        try {
            cache = true;

            cacheStore = true;

            startGridsMultiThreaded(3);

            client = true;

            cacheStore = clientStore;

            startGrid(3);

            checkStoreUpdate(clientStore);
        }
        finally {
            stopAllGrids();
        }
    }
}
