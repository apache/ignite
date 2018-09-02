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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CachePageWriteLockUnlockTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(Integer.MAX_VALUE);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * exclude metastorage: org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage#METASTORAGE_CACHE_NAME
     *
     * org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList.RemoveRowHandler
     *
     * org.apache.ignite.internal.processors.cache.persistence.DataStructure#recyclePage(long, long, long, java.lang.Boolean)
     *
     * org.apache.ignite.internal.pagemem.PageIdUtils#rotatePageId(long)
     *
     * org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord - restore new pageId on recovery.
     */
    public void testUnlock() throws Exception {
        try {
            IgniteEx grid0 = startGrid(0);

            grid0.cluster().active(true);

            grid0.cache(DEFAULT_CACHE_NAME).put(0, 0);

            grid0.cache(DEFAULT_CACHE_NAME).remove(0);

            grid0.cache(DEFAULT_CACHE_NAME).put(0, 0);

            stopGrid(0);

            grid0 = startGrid(0);

            grid0.cluster().active(true);

            grid0.cache(DEFAULT_CACHE_NAME).put(0, 0);

            grid0.cache(DEFAULT_CACHE_NAME).put(0, 0);
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }
}
