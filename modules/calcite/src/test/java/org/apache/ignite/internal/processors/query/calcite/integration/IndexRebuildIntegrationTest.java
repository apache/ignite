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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.testframework.GridTestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

/**
 * Test index rebuild.
 */
public class IndexRebuildIntegrationTest extends AbstractBasicIntegrationTest {
    /** Index rebuild init latch. */
    private static CountDownLatch initLatch;

    /** Index rebuild start latch. */
    private static CountDownLatch startLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IndexProcessor.idxRebuildCls = BlockingIndexesRebuildTask.class;

        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        initLatch = null;
        startLatch = null;

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // Override super method to skip caches destroy.
        cleanQueryPlanCache();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        super.beforeTestsStarted();

        client.cluster().state(ClusterState.ACTIVE);

        executeSql("CREATE TABLE tbl (id INT PRIMARY KEY, val VARCHAR, val2 VARCHAR) WITH CACHE_NAME=\"test\"");
        executeSql("CREATE INDEX idx_id_val ON tbl (id DESC, val)");
        executeSql("CREATE INDEX idx_id_val2 ON tbl (id, val2 DESC)");

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO tbl VALUES (?, ?, ?)", i, "val" + i, "val" + i);

        executeSql("CREATE TABLE tbl2 (id INT PRIMARY KEY, val VARCHAR)");

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO tbl2 VALUES (?, ?)", i, "val" + i);
    }

    /** */
    @Test
    public void testRebuildOnInitiatorNode() throws Exception {
        String sql = "SELECT * FROM tbl WHERE id = 0 AND val='val0'";

        QueryChecker validChecker = assertQuery(grid(0), sql)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(0, "val0", "val0");

        QueryChecker rebuildingChecker = assertQuery(grid(0), sql)
            .matches(QueryChecker.containsTableScan("PUBLIC", "TBL"))
            .returns(0, "val0", "val0");

        checkRebuildIndexQuery(grid(0), validChecker, rebuildingChecker);
    }

    /** */
    @Test
    public void testRebuildOnRemoteNodeUncorrelated() throws Exception {
        IgniteEx initNode = grid(0);

        // Uncorrelated, filter by indexed field, without projection (idenitity projection).
        for (int i = 0; i < 10; i++) {
            QueryChecker checker = assertQuery(initNode, "SELECT * FROM tbl WHERE id = ? AND val = ?")
                .withParams(i, "val" + i)
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort")))
                .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
                .returns(i, "val" + i, "val" + i);

            checkRebuildIndexQuery(grid(1), checker, checker);
        }

        // Uncorrelated, filter by indexed field, with projection.
        for (int i = 0; i < 10; i++) {
            QueryChecker checker = assertQuery(initNode, "SELECT val FROM tbl WHERE id = ? AND val = ?")
                .withParams(i, "val" + i)
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort")))
                .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
                .returns("val" + i);

            checkRebuildIndexQuery(grid(1), checker, checker);
        }
    }

    /** */
    @Test
    public void testRebuildOnRemoteNodeSorted() throws Exception {
        IgniteEx initNode = grid(0);

        // Order by part of index collation, without projection.
        String sql = "SELECT * FROM tbl WHERE id >= 10 and id <= 15 ORDER BY id DESC";

        QueryChecker checker = assertQuery(initNode, sql)
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IndexSpool")))
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort")))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(15, "val15", "val15").returns(14, "val14", "val14").returns(13, "val13", "val13")
            .returns(12, "val12", "val12").returns(11, "val11", "val11").returns(10, "val10", "val10")
            .ordered();

        checkRebuildIndexQuery(grid(1), checker, checker);

        // Order by part of index collation, with projection.
        sql = "SELECT val FROM tbl WHERE id >= 10 and id < 20 ORDER BY id DESC";

        checker = assertQuery(initNode, sql)
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IndexSpool")))
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort")))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns("val19").returns("val18").returns("val17").returns("val16").returns("val15")
            .returns("val14").returns("val13").returns("val12").returns("val11").returns("val10")
            .ordered();

        checkRebuildIndexQuery(grid(1), checker, checker);

        // Order by full index collation, with projection.
        sql = "SELECT val FROM tbl WHERE id >= 10 and id < 20 ORDER BY id DESC, val";

        checker = assertQuery(initNode, sql)
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IndexSpool")))
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort")))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns("val19").returns("val18").returns("val17").returns("val16").returns("val15")
            .returns("val14").returns("val13").returns("val12").returns("val11").returns("val10")
            .ordered();

        checkRebuildIndexQuery(grid(1), checker, checker);

        // Order by another collation.
        executeSql("CREATE INDEX idx_val ON tbl (val DESC)");

        try {
            sql = "SELECT * FROM tbl WHERE val BETWEEN 'val10' AND 'val15' ORDER BY val";

            checker = assertQuery(initNode, sql)
                .matches(QueryChecker.containsSubPlan("IgniteSort"))
                .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_VAL"))
                .returns(10, "val10", "val10").returns(11, "val11", "val11").returns(12, "val12", "val12")
                .returns(13, "val13", "val13").returns(14, "val14", "val14").returns(15, "val15", "val15")
                .ordered();

            checkRebuildIndexQuery(grid(1), checker, checker);
        }
        finally {
            executeSql("DROP INDEX idx_val");
        }
    }

    /**
     * Test min-max optimization with index rebuilding.
     */
    @Test
    public void testIndexFirstLastAtUnavailableIndex() throws IgniteCheckedException {
        int records = 50;
        int iterations = 500;

        CalciteQueryProcessor srvEngine = Commons.lookupComponent(grid(0).context(), CalciteQueryProcessor.class);

        for (boolean asc : new boolean[] {true, false}) {
            for (int backups = -1; backups < 3; ++backups) {
                String ddl = "CREATE TABLE tbl3 (id INT PRIMARY KEY, val BIGINT, val2 VARCHAR) WITH ";

                ddl += backups < 0 ? "TEMPLATE=REPLICATED" : "TEMPLATE=PARTITIONED,backups=" + backups;

                executeSql(ddl);

                executeSql("CREATE INDEX TEST_IDX ON tbl3(val " + (asc ? "asc)" : "desc)"));

                executeSql("INSERT INTO tbl3 VALUES (-1, null, null)");

                for (int i = 0; i < records; i++)
                    executeSql("INSERT INTO tbl3 VALUES (?, ?, ?)", i, i, "val" + i);

                executeSql("INSERT INTO tbl3 VALUES (" + records + ", null, null)");

                IgniteCacheTable tbl3 = (IgniteCacheTable)srvEngine.schemaHolder().schema("PUBLIC").getTable("TBL3");

                AtomicBoolean stop = new AtomicBoolean();

                tbl3.markIndexRebuildInProgress(false);

                IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                    boolean lever = true;

                    while (!stop.get())
                        tbl3.markIndexRebuildInProgress(lever = !lever);
                });

                try {
                    for (int i = 0; i < iterations; ++i) {
                        assertQuery("select MIN(val) from tbl3").returns((long)0).check();

                        assertQuery("select MAX(val) from tbl3").returns((long)(records - 1)).check();
                    }
                }
                finally {
                    stop.set(true);

                    tbl3.markIndexRebuildInProgress(false);

                    executeSql("DROP TABLE tbl3");
                }

                fut.get();
            }
        }
    }

    /**
     * Test IndexCount is disabled at index rebuilding.
     */
    @Test
    public void testIndexCountAtUnavailableIndex() throws IgniteCheckedException {
        int records = 50;
        int iterations = 500;

        CalciteQueryProcessor srvEngine = Commons.lookupComponent(grid(0).context(), CalciteQueryProcessor.class);

        for (int backups = -1; backups < 3; ++backups) {
            String ddl = "CREATE TABLE tbl3 (id INT PRIMARY KEY, val VARCHAR, val2 VARCHAR) WITH ";

            ddl += backups < 0 ? "TEMPLATE=REPLICATED" : "TEMPLATE=PARTITIONED,backups=" + backups;

            executeSql(ddl);

            executeSql("CREATE INDEX idx_val ON tbl3(val)");

            for (int i = 0; i < records; i++)
                executeSql("INSERT INTO tbl3 VALUES (?, ?, ?)", i, i % 2 == 0 ? null : "val" + i, "val" + i);

            IgniteCacheTable tbl3 = (IgniteCacheTable)srvEngine.schemaHolder().schema("PUBLIC").getTable("TBL3");

            AtomicBoolean stop = new AtomicBoolean();

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                boolean lever = true;

                while (!stop.get())
                    tbl3.markIndexRebuildInProgress(lever = !lever);
            });

            try {
                for (int i = 0; i < iterations; ++i) {
                    assertQuery("select COUNT(*) from tbl3").returns((long)records).check();
                    assertQuery("select COUNT(val) from tbl3").returns((long)records / 2L).check();
                }
            }
            finally {
                stop.set(true);

                tbl3.markIndexRebuildInProgress(false);

                executeSql("DROP TABLE tbl3");
            }

            fut.get();
        }
    }

    /** */
    @Test
    public void testRebuildOnRemoteNodeCorrelated() throws Exception {
        IgniteEx initNode = grid(0);

        // Correlated join with correlation in filter, without project.
        String sql = "SELECT /*+ CNL_JOIN */ tbl2.id, tbl.val " +
            "FROM tbl2 LEFT JOIN tbl ON tbl.id = tbl2.id AND tbl.val = tbl2.val AND tbl.id % 2 = 0 " +
            "WHERE tbl2.id BETWEEN 10 AND 19";

        QueryChecker checker = assertQuery(initNode, sql)
            .matches(QueryChecker.containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(10, "val10").returns(11, null).returns(12, "val12").returns(13, null).returns(14, "val14")
            .returns(15, null).returns(16, "val16").returns(17, null).returns(18, "val18").returns(19, null);

        checkRebuildIndexQuery(grid(1), checker, checker);

        // Correlated join with correlation in filter, with project.
        sql = "SELECT /*+ CNL_JOIN */ tbl2.id, tbl.val1 " +
            "FROM tbl2 JOIN (SELECT tbl.val || '-' AS val1, val, id FROM tbl) AS tbl " +
            "ON tbl.id = tbl2.id AND tbl.val = tbl2.val " +
            "WHERE tbl2.id BETWEEN 10 AND 12";

        checker = assertQuery(initNode, sql)
            .matches(QueryChecker.containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(10, "val10-").returns(11, "val11-").returns(12, "val12-");

        checkRebuildIndexQuery(grid(1), checker, checker);
    }

    /** */
    @Test
    public void testRebuildOnRemoteNodeCollationRestore() throws Exception {
        IgniteEx initNode = grid(0);

        // Correlated join with correlation in filter, with project as a subset of collation.
        String sql = "SELECT /*+ CNL_JOIN */ tbl2.id, tbl.id1 " +
            "FROM tbl2 JOIN (SELECT tbl.id + 1 AS id1, id FROM tbl WHERE val >= 'val') AS tbl " +
            "ON tbl.id = tbl2.id " +
            "WHERE tbl2.val BETWEEN 'val10' AND 'val12'";

        QueryChecker checker = assertQuery(initNode, sql)
            .matches(QueryChecker.containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(10, 11).returns(11, 12).returns(12, 13);

        checkRebuildIndexQuery(grid(1), checker, checker);

        // Correlated join with correlation in filter, with a project as a subset of collation with DESC ordering.
        sql = "SELECT /*+ CNL_JOIN */ tbl2.id, tbl.id1 " +
            "FROM tbl2 JOIN (SELECT tbl.id + 1 AS id1, id FROM tbl WHERE val2 >= 'val') AS tbl " +
            "ON tbl.id = tbl2.id " +
            "WHERE tbl2.val BETWEEN 'val10' AND 'val12'";

        checker = assertQuery(initNode, sql)
            .matches(QueryChecker.containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL2"))
            .returns(10, 11).returns(11, 12).returns(12, 13);

        checkRebuildIndexQuery(grid(1), checker, checker);
    }

    /** */
    private void checkRebuildIndexQuery(
        IgniteEx rebuildNode,
        QueryChecker checkerWhenValid,
        QueryChecker checkerWhenRebuilding
    ) throws Exception {
        initLatch = new CountDownLatch(1);
        startLatch = new CountDownLatch(1);

        GridCacheContext<?, ?> cctx = rebuildNode.context().cache().cache("test").context();

        checkerWhenValid.check();

        GridTestUtils.runAsync(() -> forceRebuildIndexes(rebuildNode, cctx));

        try {
            initLatch.await(10, TimeUnit.SECONDS);

            // Check query cache invalidated after index rebuild start.
            checkerWhenRebuilding.check();
        }
        finally {
            startLatch.countDown();
        }

        indexRebuildFuture(rebuildNode, cctx.cacheId()).get();

        // Check query cache invalidated after index rebuild finish.
        checkerWhenValid.check();
    }

    /** */
    static class BlockingIndexesRebuildTask extends IndexesRebuildTask {
        /** {@inheritDoc} */
        @Override protected void startRebuild(
            GridCacheContext cctx,
            GridFutureAdapter<Void> fut,
            SchemaIndexCacheVisitorClosure clo,
            IndexRebuildCancelToken cancelTok
        ) {
            try {
                if (initLatch != null)
                    initLatch.countDown();

                if (startLatch != null)
                    startLatch.await(100, TimeUnit.SECONDS);
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
            super.startRebuild(cctx, fut, clo, cancelTok);
        }
    }
}
