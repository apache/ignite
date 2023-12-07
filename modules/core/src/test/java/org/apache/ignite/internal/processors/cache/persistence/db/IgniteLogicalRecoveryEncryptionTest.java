/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.db;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Class containing tests for logical recovery of encrypted caches.
 */
public class IgniteLogicalRecoveryEncryptionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
        encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

        return super.getConfiguration(igniteInstanceName)
            .setEncryptionSpi(encSpi)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                ));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests a scenario that partition states of encrypted caches are successfully restored by logical recovery.
     */
    @Test
    public void testRecoverPartitionStates() throws Exception {
        IgniteEx ignite = startTestGrid();

        // Disable checkpoints to force logical recovery
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite
            .context()
            .cache()
            .context()
            .database();

        dbMgr.enableCheckpoints(false).get();

        // Create a dynamic cache to ensure that partition states get restored (static caches may skip that step)
        CacheConfiguration<Integer, Integer> cacheCfg = GridAbstractTest.<Integer, Integer>defaultCacheConfiguration()
            .setEncryptionEnabled(true);

        IgniteCache<Integer, Integer> cache = ignite.createCache(cacheCfg);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        stopAllGrids();

        ignite = startTestGrid();

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            assertEquals(i, cache.get(i).intValue());
    }

    /**
     * Creates a cluster of two nodes.
     */
    private IgniteEx startTestGrid() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        return ignite;
    }
}
