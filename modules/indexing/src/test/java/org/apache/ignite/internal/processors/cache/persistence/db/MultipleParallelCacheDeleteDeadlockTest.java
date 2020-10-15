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
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 *
 */
public class MultipleParallelCacheDeleteDeadlockTest extends GridCommonAbstractTest {
    /** Latch that blocks test completion. */
    private CountDownLatch testCompletionBlockingLatch = new CountDownLatch(1);

    /** Latch that blocks checkpoint. */
    private CountDownLatch checkpointBlockingLatch = new CountDownLatch(1);

    /** We imitate long index destroy in these tests, so this is delay for each page to destroy. */
    private static final long TIME_FOR_EACH_INDEX_PAGE_TO_DESTROY = 300;

    /** */
    private static final String CACHE_1 = "cache_1";

    /** */
    private static final String CACHE_2 = "cache_2";

    /** */
    private static final String CACHE_GRP_1 = "cache_grp_1";

    /** */
    private static final String CACHE_GRP_2 = "cache_grp_2";

    /** */
    private H2TreeIndex.H2TreeFactory regularH2TreeFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(10 * 1024L * 1024L)
                        .setMaxSize(50 * 1024L * 1024L)
                )
                .setCheckpointFrequency(Integer.MAX_VALUE)
            )
            .setCacheConfiguration(
                new CacheConfiguration(CACHE_1)
                    .setGroupName(CACHE_GRP_1)
                    .setSqlSchema("PUBLIC"),
                new CacheConfiguration(CACHE_2)
                    .setGroupName(CACHE_GRP_2)
                    .setSqlSchema("PUBLIC")
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        regularH2TreeFactory = H2TreeIndex.h2TreeFactory;

        H2TreeIndex.h2TreeFactory = H2TreeTest::new;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        H2TreeIndex.h2TreeFactory = regularH2TreeFactory;

        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testMultipleCacheDelete() throws Exception {
        IgniteEx ignite = startGrids(1);

        ignite.cluster().active(true);

        IgniteCache cache1 = ignite.getOrCreateCache(CACHE_1);
        IgniteCache cache2 = ignite.getOrCreateCache(CACHE_2);

        query(cache1, "create table t1(id integer primary key, f integer) with \"CACHE_GROUP=" + CACHE_GRP_1 + "\"");
        query(cache1, "create index idx1 on t1(f)");

        for (int i = 0; i < 500; i++)
            query(cache1, "insert into t1 (id, f) values (?, ?)", i, i);

        query(cache2, "create table t2(id integer primary key, f integer) with \"CACHE_GROUP=" + CACHE_GRP_2 + "\"");
        query(cache2, "create index idx2 on t2(f)");

        for (int i = 0; i < 500; i++)
            query(cache2, "insert into t2 (id, f) values (?, ?)", i, i);

        Thread checkpointer = new Thread(() -> {
            try {
                checkpointBlockingLatch.await();

                forceCheckpoint();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                testCompletionBlockingLatch.countDown();
            }
        });

        Thread destroyCaches = new Thread(() -> {
            ignite.destroyCaches(asList("SQL_PUBLIC_T1", "SQL_PUBLIC_T2"));
        });

        checkpointer.start();
        destroyCaches.start();

        testCompletionBlockingLatch.await(60, TimeUnit.SECONDS);

        try {
            assertEquals("Test hasn't completed in 1 minute - there is possibly a deadlock.", 0, testCompletionBlockingLatch.getCount());
        }
        finally {
            checkpointer.interrupt();
            destroyCaches.interrupt();

            checkpointer.join();
            destroyCaches.join();
        }
    }

    /**
     * Does single query.
     *
     * @param cache Cache.
     * @param qry Query.
     * @return Query result.
     */
    private List<List<?>> query(IgniteCache<Integer, Integer> cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /**
     * Does parametrized query.
     *
     * @param cache Cache.
     * @param qry Query.
     * @param args Arguments.
     * @return Query result.
     */
    private List<List<?>> query(IgniteCache<Integer, Integer> cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /**
     * Test H2 tree.
     */
    private class H2TreeTest extends H2Tree {
        /**
         * Constructor.
         *
         * @param cctx Cache context.
         * @param table Owning table.
         * @param name Tree name.
         * @param idxName Name of index.
         * @param cacheName Cache name.
         * @param tblName Table name.
         * @param reuseList Reuse list.
         * @param grpId Cache group ID.
         * @param grpName
         * @param pageMem Page memory.
         * @param wal Write ahead log manager.
         * @param globalRmvId
         * @param metaPageId Meta page ID.
         * @param initNew Initialize new index.
         * @param unwrappedCols Unwrapped columns.
         * @param wrappedCols Wrapped columns.
         * @param maxCalculatedInlineSize
         * @param pk {@code true} for primary key.
         * @param affinityKey {@code true} for affinity key.
         * @param mvccEnabled Mvcc flag.
         * @param rowCache Row cache.
         * @param failureProcessor if the tree is corrupted.
         * @param log Logger.
         * @param stats Statistics holder.
         * @throws IgniteCheckedException If failed.
         */
        public H2TreeTest(
            GridCacheContext cctx,
            GridH2Table table,
            String name,
            String idxName,
            String cacheName,
            String tblName,
            ReuseList reuseList,
            int grpId,
            String grpName,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            AtomicLong globalRmvId,
            long metaPageId,
            boolean initNew,
            List<IndexColumn> unwrappedCols,
            List<IndexColumn> wrappedCols,
            AtomicInteger maxCalculatedInlineSize,
            boolean pk,
            boolean affinityKey,
            boolean mvccEnabled,
            @Nullable H2RowCache rowCache,
            @Nullable FailureProcessor failureProcessor,
            IgniteLogger log,
            IoStatisticsHolder stats,
            InlineIndexColumnFactory factory,
            int configuredInlineSize
        ) throws IgniteCheckedException {
            super(
                cctx,
                table,
                name,
                idxName,
                cacheName,
                tblName,
                reuseList,
                grpId,
                grpName,
                pageMem,
                wal,
                globalRmvId,
                metaPageId,
                initNew,
                unwrappedCols,
                wrappedCols,
                maxCalculatedInlineSize,
                pk,
                affinityKey,
                mvccEnabled,
                rowCache,
                failureProcessor,
                log,
                stats,
                factory,
                configuredInlineSize
            );
        }

        /** {@inheritDoc} */
        @Override protected long destroyDownPages(
            LongListReuseBag bag,
            long pageId,
            int lvl,
            IgniteInClosure<H2Row> c,
            AtomicLong lockHoldStartTime,
            long lockMaxTime,
            Deque<GridTuple3<Long, Long, Long>> lockedPages
        ) throws IgniteCheckedException {
            doSleep(TIME_FOR_EACH_INDEX_PAGE_TO_DESTROY);

            return super.destroyDownPages(bag, pageId, lvl, c, lockHoldStartTime, lockMaxTime, lockedPages);
        }

        /** {@inheritDoc} */
        @Override protected void temporaryReleaseLock() {
            super.temporaryReleaseLock();

            checkpointBlockingLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override protected long maxLockHoldTime() {
            return 10;
        }
    }
}
