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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

import static org.apache.ignite.configuration.PersistentStoreConfiguration.DFLT_WAL_SEGMENTS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgniteWalReaderTest extends GridCommonAbstractTest {

    private static String cacheName = "cache0";

    private static boolean fillWalBeforeTest = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IgniteWalReaderTest.IndexedObject> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        // ccfg.setNodeFilter(new IgniteWalRecoveryTest.RemoteNodeFilter());
        ccfg.setIndexedTypes(Integer.class, IgniteWalReaderTest.IndexedObject.class);

        cfg.setCacheConfiguration(ccfg);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setPageSize(4 * 1024);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(1024 * 1024 * 1024);
        memPlcCfg.setMaxSize(1024 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();

        cfg.setPersistentStoreConfiguration(pCfg);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        if (fillWalBeforeTest)
            deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalAndReadRecords() throws Exception {
        if (fillWalBeforeTest) {
            Ignite ignite0 = startGrid("node0");
            ignite0.active(true);

            IgniteCache<Object, Object> cache0 = ignite0.cache(cacheName);

            for (int i = 0; i < 100; i++)
                cache0.put(i, new IgniteWalReaderTest.IndexedObject(i));

            stopGrid("node0");
        }

        File db = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File wal = new File(db, "wal");
        File walArchive = new File(wal, "archive");

        int pageSize = 1024 * 4;
        PersistentStoreConfiguration persistentCfg1 = Mockito.mock(PersistentStoreConfiguration.class);
        when(persistentCfg1.getWalStorePath()).thenReturn(wal.getAbsolutePath());
        when(persistentCfg1.getWalArchivePath()).thenReturn(walArchive.getAbsolutePath());
        int segments = DFLT_WAL_SEGMENTS;
        String consistentId = "127_0_0_1_47500";
        when(persistentCfg1.getWalSegments()).thenReturn(segments);
        when(persistentCfg1.getTlbSize()).thenReturn(PersistentStoreConfiguration.DFLT_TLB_SIZE);
        when(persistentCfg1.getWalRecordIteratorBufferSize()).thenReturn(PersistentStoreConfiguration.DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE);

        PersistentStoreConfiguration persistentCfg2 = new PersistentStoreConfiguration();
        persistentCfg2.setWalArchivePath("C:\\projects\\incubator-ignite\\work\\db\\wal");
        persistentCfg2.setWalArchivePath("C:\\projects\\incubator-ignite\\work\\db\\wal\\archive");

        IgniteConfiguration cfg = Mockito.mock(IgniteConfiguration.class);
        when(cfg.getPersistentStoreConfiguration()).thenReturn(persistentCfg1);

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        when(ctx.config()).thenReturn(cfg);
        when(ctx.clientNode()).thenReturn(false);

        GridDiscoveryManager disco = Mockito.mock(GridDiscoveryManager.class);
        when(disco.consistentId()).thenReturn(consistentId);
        when(ctx.discovery()).thenReturn(disco);

        FileWriteAheadLogManager mgr = new FileWriteAheadLogManager(ctx);
        GridCacheSharedContext sctx = Mockito.mock(GridCacheSharedContext.class);
        when(sctx.kernalContext()).thenReturn(ctx);
        when(sctx.discovery()).thenReturn(disco);
        GridCacheDatabaseSharedManager database = Mockito.mock(GridCacheDatabaseSharedManager.class);
        when(database.pageSize()).thenReturn(pageSize);
        when(sctx.database()).thenReturn(database);
        when(sctx.logger(any(Class.class))).thenReturn(Mockito.mock(IgniteLogger.class));

        mgr.start(sctx);
        WALIterator it = mgr.replay(null);
        for (; it.hasNextX(); ) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();
            System.out.println("Record: " + next.get2());
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IgniteWalReaderTest.IndexedObject))
                return false;

            IgniteWalReaderTest.IndexedObject that = (IgniteWalReaderTest.IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IgniteWalReaderTest.IndexedObject.class, this);
        }
    }
}
