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

package org.apache.ignite.cache.query;

import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;

/** */
@RunWith(Parameterized.class)
public class IndexQueryRebuildIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "TEST_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private boolean persistenceEnabled;

    /** */
    @Parameterized.Parameter
    public String qryNode;

    /** */
    @Parameterized.Parameters(name = "qryNode={0}")
    public static Object[] parameters() {
        return new Object[] { "CRD", "CLN" };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryEntity qe = new QueryEntity(Long.class.getName(), Integer.class.getName())
            .setTableName("Person")
            .setKeyFieldName("id")
            .setValueFieldName("fld")
            .setFields(new LinkedHashMap<>(
                F.asMap("id", Long.class.getName(), "fld", Integer.class.getName()))
            );

        CacheConfiguration<Long, Integer> ccfg = new CacheConfiguration<Long, Integer>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(F.asList(qe))
            .setBackups(1);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testConcurrentCreateIndex() throws Exception {
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        startGrids(3);

        IgniteCache<Long, Integer> cache = cache();

        insertData();

        BlockingIndexing idx0 = (BlockingIndexing)grid(0).context().query().getIndexing();

        multithreadedAsync(() -> {
            idx0.setUp();

            SqlFieldsQuery idxCreate = new SqlFieldsQuery("create index " + IDX + " on Person(fld)");

            cache.query(idxCreate).getAll();
        }, 1);

        IndexQuery<Long, Integer> qry = new IndexQuery<Long, Integer>(Integer.class)
            .setCriteria(between("fld", 0, CNT));

        GridTestUtils.assertThrows(null,
            () -> {
                idx0.idxBuildStartLatch.await();

                cache.query(qry).getAll();
            }, IgniteException.class, "Failed to run IndexQuery: index " + IDX + " isn't completed yet.");

        idx0.blockIdxBuildLatch.countDown();

        idx0.idxCreatedLatch.await();

        assertEquals(CNT, cache.query(qry).getAll().size());
    }

    /** */
    @Test
    public void testConcurrentRebuildIndex() throws Exception {
        persistenceEnabled = true;

        IndexProcessor.idxRebuildCls = BlockingRebuildIndexes.class;

        Ignite crd = startGrids(3);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Long, Integer> cache = cache();

        cache.query(new SqlFieldsQuery("create index " + IDX + " on Person(fld)")).getAll();

        insertData();

        BlockingRebuildIndexes rebuild = (BlockingRebuildIndexes)grid(0).context().indexProcessor().idxRebuild();

        rebuild.setUp();

        multithreadedAsync(() -> {
            forceRebuildIndexes(grid(0), grid(0).cachex(CACHE).context());
        }, 1);

        rebuild.idxRebuildStartLatch.await();

        IndexQuery<Long, Integer> qry = new IndexQuery<Long, Integer>(Integer.class)
            .setCriteria(between("fld", 0, CNT));

        GridTestUtils.assertThrows(null,
            () -> {
                cache.query(qry).getAll();
            }, IgniteException.class, "Failed to run IndexQuery: index " + IDX + " isn't completed yet.");

        rebuild.blockIdxRebuildLatch.countDown();

        crd.cache(CACHE).indexReadyFuture().get();

        assertEquals(CNT, cache.query(qry).getAll().size());
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Long, Integer> streamer = grid(0).dataStreamer(CACHE)) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long)i, i);
        }
    }

    /** */
    private IgniteCache<Long, Integer> cache() throws Exception {
        Ignite n = "CRD".equals(qryNode) ? grid(0) : startClientGrid();

        return n.cache(CACHE);
    }

    /** Blocks filling dynamically created index with cache data. */
    public static class BlockingRebuildIndexes extends IndexesRebuildTask {
        /** */
        public volatile CountDownLatch blockIdxRebuildLatch = new CountDownLatch(0);

        /** */
        public volatile CountDownLatch idxRebuildStartLatch = new CountDownLatch(0);

        /** {@inheritDoc} */
        @Override protected void startRebuild(
            GridCacheContext cctx,
            GridFutureAdapter<Void> fut,
            SchemaIndexCacheVisitorClosure clo,
            IndexRebuildCancelToken cancelTok
        ) {
            try {
                idxRebuildStartLatch.countDown();

                blockIdxRebuildLatch.await();

                super.startRebuild(cctx, fut, clo, cancelTok);
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }
        }

        /** */
        public void setUp() {
            blockIdxRebuildLatch = new CountDownLatch(1);
            idxRebuildStartLatch = new CountDownLatch(1);
        }
    }

    /** Blocks filling dynamically created index with cache data. */
    public static class BlockingIndexing extends GridQueryProcessor {
        /** */
        public volatile CountDownLatch blockIdxBuildLatch = new CountDownLatch(0);

        /** */
        public volatile CountDownLatch idxBuildStartLatch = new CountDownLatch(0);

        /** */
        public volatile CountDownLatch idxCreatedLatch = new CountDownLatch(0);

        /**
         * Constructor.
         *
         * @param ctx Kernal context.
         */
        public BlockingIndexing(GridKernalContext ctx) throws IgniteCheckedException {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> dynamicIndexCreate(
            String cacheName,
            String schemaName,
            String tblName,
            QueryIndex idx,
            boolean ifNotExists,
            int parallel
        ) {
            SchemaIndexCacheVisitor blockVisitor = new SchemaIndexCacheVisitor() {
                @Override public void visit(SchemaIndexCacheVisitorClosure clo) {
                    SchemaIndexCacheVisitorClosure blkClo = new SchemaIndexCacheVisitorClosure() {
                        @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
                            try {
                                idxBuildStartLatch.countDown();

                                blockIdxBuildLatch.await();
                            }
                            catch (InterruptedException e) {
                                throw new IgniteException(e);
                            }

                            clo.apply(row);
                        }
                    };

                    cacheVisitor.visit(blkClo);
                }
            };

            super.dynamicIndexCreate(schemaName, tblName, idxDesc, ifNotExists, blockVisitor);

            idxCreatedLatch.countDown();
        }

        /** */
        public void setUp() {
            blockIdxBuildLatch = new CountDownLatch(1);
            idxCreatedLatch = new CountDownLatch(1);
            idxBuildStartLatch = new CountDownLatch(1);
        }
    }
}
