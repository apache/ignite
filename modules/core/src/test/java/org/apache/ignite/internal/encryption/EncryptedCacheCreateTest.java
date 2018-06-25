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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.encryption.EncryptionKey;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;

/** */
public class EncryptedCacheCreateTest extends AbstractEncryptionTest {
    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        IgniteEx igniteEx = startGrid(0);

        startGrid(1);

        igniteEx.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = cfg.getDataStorageConfiguration();

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(10L * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testCreateEncryptedCache() throws Exception {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(ENCRYPTED_CACHE);

        ccfg.setEncrypted(true);

        IgniteEx grid = grid(0);

        grid.createCache(ccfg);

        IgniteInternalCache<Object, Object> encrypted = grid.cachex(ENCRYPTED_CACHE);

        EncryptionKey<?> key = encrypted.context().shared().database().groupKey(CU.cacheGroupId(ENCRYPTED_CACHE, null));

        assertNotNull(key);
        assertNotNull(key.key());
    }

    /** @throws Exception If failed. */
    public void testCreateEncryptedNotPersistedCacheFail() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8640");

        GridTestUtils.assertThrowsWithCause(() -> {
            CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(NO_PERSISTENCE_REGION);

            ccfg.setEncrypted(true);
            ccfg.setDataRegionName(NO_PERSISTENCE_REGION);

            grid(0).createCache(ccfg);
        }, IgniteCheckedException.class);
    }
}
