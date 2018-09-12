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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteWalRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final String HAS_CACHE = "HAS_CACHE";

    /** */
    private static final int LARGE_ARR_SIZE = 1025;

    /** */
    private boolean fork;

    /** */
    private static final String CACHE_NAME = "partitioned";

    /** */
    private static final String RENAMED_CACHE_NAME = "partitioned0";

    /** */
    private static final String LOC_CACHE_NAME = "local";

    /** */
    private boolean renamed;

    /** */
    private int walSegmentSize;

    /** Log only. */
    private boolean logOnly;

    /** */
    private boolean prevDumpOnFailure;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return fork;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = renamed ?
            new CacheConfiguration<>(RENAMED_CACHE_NAME) : new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setNodeFilter(new RemoteNodeFilter());
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);

        CacheConfiguration<Integer, IndexedObject> locCcfg = new CacheConfiguration<>(LOC_CACHE_NAME);
        locCcfg.setCacheMode(CacheMode.LOCAL);
        locCcfg.setIndexedTypes(Integer.class, IndexedObject.class);

        cfg.setCacheConfiguration(ccfg, locCcfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(4 * 1024);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();

        memPlcCfg.setName("dfltDataRegion");
        memPlcCfg.setInitialSize(1024L * 1024 * 1024);
        memPlcCfg.setMaxSize(1024L * 1024 * 1024);
        memPlcCfg.setPersistenceEnabled(true);

        dbCfg.setDefaultDataRegionConfiguration(memPlcCfg);

        dbCfg.setWalRecordIteratorBufferSize(1024 * 1024);

        dbCfg.setWalHistorySize(2);

        if (logOnly)
            dbCfg.setWalMode(WALMode.LOG_ONLY);

        if (walSegmentSize != 0)
            dbCfg.setWalSegmentSize(walSegmentSize);

        cfg.setDataStorageConfiguration(dbCfg);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        if (!getTestIgniteInstanceName(0).equals(gridName))
            cfg.setUserAttributes(F.asMap(HAS_CACHE, true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        prevDumpOnFailure = IgniteSystemProperties.getBoolean(IGNITE_DUMP_THREADS_ON_FAILURE);

        System.setProperty(IGNITE_DUMP_THREADS_ON_FAILURE, "true");

        stopAllGrids();

        cleanPersistenceDir();

        renamed = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        logOnly = false;

        cleanPersistenceDir();

        System.setProperty(IGNITE_DUMP_THREADS_ON_FAILURE, String.valueOf(prevDumpOnFailure));
    }

    /**
     * @throws Exception if failed.
     */
    public void testHugeCheckpointRecord() throws Exception {
        try {
            final IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            for (int i = 0; i < 50; i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("cache-" + i);

                // We can get 'too many open files' with default number of partitions.
                ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));

                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

                cache.put(i, i);
            }

            final long endTime = System.currentTimeMillis() + 30_000;

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() {
                    Random rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < endTime) {
                        IgniteCache<Object, Object> cache = ignite.cache("cache-" + rnd.nextInt(50));

                        cache.put(rnd.nextInt(50_000), rnd.nextInt());
                    }

                    return null;
                }
            }, 16, "put-thread");

            while (System.currentTimeMillis() < endTime) {
                ignite.context().cache().context().database().wakeupForCheckpoint("test").get();

                U.sleep(500);
            }

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class RemoteNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.attribute(HAS_CACHE) != null;
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

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }

    /**
     *
     */
    private enum EnumVal {
        /** */
        VAL1,

        /** */
        VAL2,

        /** */
        VAL3
    }
}
