/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.backup.IgniteBackupPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileBackupDescriptor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class IgnitePdsCachePartitonsBackupSelfTest extends GridCommonAbstractTest {
    /** */
    @Before
    public void beforeTestCleanup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void afterTestCleanup() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(64));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception Exception.
     */
    @Test
    public void testCopyCachePartitonFiles() throws Exception {
        final IgniteEx ignite0 = (IgniteEx)startGrids(2);

        ignite0.cluster().active(true);

        for (int i = 0; i < 2048; i++)
            ignite0.cache(DEFAULT_CACHE_NAME).put(i, i);

        awaitPartitionMapExchange();

        IgniteBackupPageStoreManager<FileBackupDescriptor> backup = ignite0.context().cache().context().storeBackup();

        backup.backup(
            1,
            CU.cacheId(DEFAULT_CACHE_NAME),
            new HashSet<>(Arrays.asList(1, 2, 3)),
            new BackupProcessTask<FileBackupDescriptor>() {
                @Override public void handlePartition(FileBackupDescriptor descr) throws IgniteCheckedException {

                }

                @Override public void handleDelta(FileBackupDescriptor descr) throws IgniteCheckedException {

                }
            });
    }
}
