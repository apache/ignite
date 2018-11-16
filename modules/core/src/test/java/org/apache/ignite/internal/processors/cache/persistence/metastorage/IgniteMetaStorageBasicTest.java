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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
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
     *
     */
    public void testMetaStorageMassivePutFixed() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

        MetaStorage metaStorage = db.metaStorage();

        assertNotNull(metaStorage);

        Random rnd = new Random();

        db.checkpointReadLock();

        int size;
        try {
            for (int i = 0; i < 10_000; i++) {
                size = rnd.nextBoolean() ? 3500 : 2 * 3500;
                String key = "TEST_KEY_" + (i % 1000);

                byte[] arr = new byte[size];
                rnd.nextBytes(arr);

                metaStorage.remove(key);

                metaStorage.putData(key, arr/*b.toString().getBytes()*/);
            }
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     *
     */
    public void testMetaStorageMassivePutRandom() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager db = ig.context().cache().context().database();

        MetaStorage metaStorage = db.metaStorage();

        assertNotNull(metaStorage);

        Random rnd = new Random();

        db.checkpointReadLock();

        int size;
        try {
            for (int i = 0; i < 50_000; i++) {
                size = 100 + rnd.nextInt(9000);

                String key = "TEST_KEY_" + (i % 2_000);

                byte[] arr = new byte[size];
                rnd.nextBytes(arr);

                metaStorage.remove(key);

                metaStorage.putData(key, arr/*b.toString().getBytes()*/);
            }
        }
        finally {
            db.checkpointReadUnlock();
        }
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

    /**
     * @throws Exception If fails.
     */
    public void testRecoveryOfMetastorageWhenNodeNotInBaseline() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        final byte KEYS_CNT = 100;
        final String KEY_PREFIX = "test.key.";
        final String NEW_VAL_PREFIX = "new.val.";
        final String UPDATED_VAL_PREFIX = "updated.val.";

        startGrid(1);

        // Disable checkpoints in order to check whether recovery works.
        forceCheckpoint(grid(1));
        disableCheckpoints(grid(1));

        loadKeys(grid(1), KEYS_CNT, KEY_PREFIX, NEW_VAL_PREFIX, UPDATED_VAL_PREFIX);

        stopGrid(1, true);

        startGrid(1);

        verifyKeys(grid(1), KEYS_CNT, KEY_PREFIX, UPDATED_VAL_PREFIX);
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

    /**
     * Disable checkpoints on a specific node.
     *
     * @param node Ignite node.h
     * @throws IgniteCheckedException If failed.
     */
    private void disableCheckpoints(Ignite node) throws IgniteCheckedException {
        assert !node.cluster().localNode().isClient();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)node).context()
                .cache().context().database();

        dbMgr.enableCheckpoints(false).get();
    }
}
