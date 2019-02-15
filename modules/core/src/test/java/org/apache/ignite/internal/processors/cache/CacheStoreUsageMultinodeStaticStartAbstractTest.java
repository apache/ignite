/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    public void testStaticConfigurationLocalStore() throws Exception {
        locStore = true;

        checkStoreUpdateStaticConfig(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationLocalStoreNearEnabled() throws Exception {
        locStore = true;

        nearCache = true;

        checkStoreUpdateStaticConfig(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationTxLocalStoreNoClientStore() throws Exception {
        locStore = true;

        checkStoreUpdateStaticConfig(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationTxLocalStoreNoClientStoreNearEnabled() throws Exception {
        locStore = true;

        nearCache = true;

        checkStoreUpdateStaticConfig(false);
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
