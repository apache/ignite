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
package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Single place to add for basic MetaStorage tests.
 */
public class IgniteMetaStorageBasicTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(storageCfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Verifies that MetaStorage after massive amounts of keys stored and updated keys restores its state successfully
     * after restart.
     *
     * See <a hfer="https://issues.apache.org/jira/browse/IGNITE-7964" target="_top">IGNITE-7964</a> for more context
     * about this test.
     *
     * @throws Exception If failed.
     */
    public void testMetaStorageMassivePutUpdateRestart() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        final byte KEYS_CNT = 100;
        final String KEY_PREFIX = "test.key.";
        final String NEW_VAL_PREFIX = "new.val.";
        final String UPDATED_VAL_PREFIX = "updated.val.";

        loadKeys(ig, KEYS_CNT, KEY_PREFIX, NEW_VAL_PREFIX, UPDATED_VAL_PREFIX);

        stopGrid(0);

        ig = startGrid(0);

        ig.cluster().active(true);

        verifyKeys(ig, KEYS_CNT, KEY_PREFIX, UPDATED_VAL_PREFIX);
    }

    /** */
    private void loadKeys(IgniteEx ig,
        byte keysCnt,
        String keyPrefix,
        String newValPrefix,
        String updatedValPrefix
    ) throws IgniteCheckedException {
        IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

        MetaStorage metaStorage = db.metaStorage();

        db.checkpointReadLock();
        try {
            for (byte i = 0; i < keysCnt; i++)
                metaStorage.write(keyPrefix + i, newValPrefix + i);

            for (byte i = 0; i < keysCnt; i++)
                metaStorage.write(keyPrefix + i, updatedValPrefix + i);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /** */
    private void verifyKeys(IgniteEx ig,
        byte keysCnt,
        String keyPrefix,
        String valPrefix
    ) throws IgniteCheckedException {
        MetaStorage metaStorage = ig.context().cache().context().database().metaStorage();

        for (byte i = 0; i < keysCnt; i++) {
            Serializable val = metaStorage.read(keyPrefix + i);

            Assert.assertEquals(valPrefix + i, val);
        }
    }
}
