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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.InlineIndexTreeFactory;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineRecommender;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.idxTreeFactory;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;

/**
 *
 */
public class MultipleParallelCacheDeleteDeadlockTest extends GridCommonAbstractTest {
    /** Latch that blocks test completion. */
    private final CountDownLatch testCompletionBlockingLatch = new CountDownLatch(1);

    /** Latch that blocks checkpoint. */
    private final CountDownLatch checkpointBlockingLatch = new CountDownLatch(1);

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

    /** Original {@link DurableBackgroundCleanupIndexTreeTaskV2#idxTreeFactory}. */
    private InlineIndexTreeFactory originalFactory;

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
                    .setSqlSchema(DFLT_SCHEMA),
                new CacheConfiguration(CACHE_2)
                    .setGroupName(CACHE_GRP_2)
                    .setSqlSchema(DFLT_SCHEMA)
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        originalFactory = idxTreeFactory;
        idxTreeFactory = new InlineIndexTreeFactoryEx();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        idxTreeFactory = originalFactory;
        originalFactory = null;

        super.afterTest();
    }

    /** */
    @Test
    public void testMultipleCacheDelete() throws Exception {
        IgniteEx ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);

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
     * Test Inline index tree.
     */
    private class InlineIndexTreeTest extends InlineIndexTree {
        /** */
        public InlineIndexTreeTest(
            SortedIndexDefinition def,
            CacheGroupContext grpCtx,
            String treeName,
            IgniteCacheOffheapManager offheap,
            ReuseList reuseList,
            PageMemory pageMemory,
            PageIoResolver pageIoResolver,
            long metaPageId,
            boolean initNew,
            int configuredInlineSize,
            int maxInlineSize,
            IndexKeyTypeSettings keyTypeSettings,
            IndexRowCache rowCache,
            IoStatisticsHolder stats,
            InlineIndexRowHandlerFactory rowHndFactory,
            InlineRecommender recommender
        ) throws IgniteCheckedException {
            super(
                def,
                grpCtx,
                treeName,
                offheap,
                reuseList,
                pageMemory,
                pageIoResolver,
                metaPageId,
                initNew,
                configuredInlineSize,
                maxInlineSize,
                keyTypeSettings,
                rowCache,
                stats,
                rowHndFactory,
                recommender
            );
        }

        /** {@inheritDoc} */
        @Override protected long destroyDownPages(
            LongListReuseBag bag,
            long pageId,
            int lvl,
            IgniteInClosure<IndexRow> c,
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

    /**
     * Extension {@link InlineIndexTreeFactory} for test.
     */
    private class InlineIndexTreeFactoryEx extends InlineIndexTreeFactory {
        /** {@inheritDoc} */
        @Override protected InlineIndexTree create(
            CacheGroupContext grpCtx,
            RootPage rootPage,
            String treeName
        ) throws IgniteCheckedException {
            return new InlineIndexTreeTest(
                null,
                grpCtx,
                treeName,
                grpCtx.offheap(),
                grpCtx.offheap().reuseListForIndex(treeName),
                grpCtx.dataRegion().pageMemory(),
                PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
                rootPage.pageId().pageId(),
                false,
                0,
                0,
                new IndexKeyTypeSettings(),
                null,
                null,
                new DurableBackgroundCleanupIndexTreeTaskV2.NoopRowHandlerFactory(),
                null
            );
        }
    }
}
