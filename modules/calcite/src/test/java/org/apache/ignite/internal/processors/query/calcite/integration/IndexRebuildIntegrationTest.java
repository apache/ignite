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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
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

        executeSql("CREATE TABLE tbl (id INT PRIMARY KEY, val VARCHAR) WITH CACHE_NAME=\"test\"");
        executeSql("CREATE INDEX idx_id_val ON tbl (id DESC, val)");
        executeSql("CREATE INDEX idx_val ON tbl (val DESC)");

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO tbl VALUES (?, ?)", i, "val" + i);

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
            .returns(0, "val0");

        QueryChecker rebuildingChecker = assertQuery(grid(0), sql)
            .matches(QueryChecker.containsTableScan("PUBLIC", "TBL"))
            .returns(0, "val0");

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
                .returns(i, "val" + i);

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
        String sql = "SELECT * FROM tbl WHERE id >= 10 and id < 20 ORDER BY id DESC";

        QueryChecker checker = assertQuery(initNode, sql)
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IndexSpool")))
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort")))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(19, "val19").returns(18, "val18").returns(17, "val17").returns(16, "val16").returns(15, "val15")
            .returns(14, "val14").returns(13, "val13").returns(12, "val12").returns(11, "val11").returns(10, "val10")
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
        sql = "SELECT * FROM tbl WHERE val BETWEEN 'val10' AND 'val19' ORDER BY val";

        checker = assertQuery(initNode, sql)
            .matches(QueryChecker.containsSubPlan("IgniteSort"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_VAL"))
            .returns(10, "val10").returns(11, "val11").returns(12, "val12").returns(13, "val13").returns(14, "val14")
            .returns(15, "val15").returns(16, "val16").returns(17, "val17").returns(18, "val18").returns(19, "val19")
            .ordered();

        checkRebuildIndexQuery(grid(1), checker, checker);
    }

    /** */
    @Test
    public void testRebuildOnRemoteNodeCorrelated() throws Exception {
        IgniteEx initNode = grid(0);

        // Correlated join with correlation in filter, without project.
        String sql = "SELECT /*+ DISABLE_RULE('MergeJoinConverter', 'NestedLoopJoinConverter') */ tbl2.id, tbl.val " +
            "FROM tbl2 LEFT JOIN tbl ON tbl.id = tbl2.id AND tbl.val = tbl2.val AND tbl.id % 2 = 0 " +
            "WHERE tbl2.id BETWEEN 10 AND 19";

        QueryChecker checker = assertQuery(initNode, sql)
            .matches(QueryChecker.containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "TBL", "IDX_ID_VAL"))
            .returns(10, "val10").returns(11, null).returns(12, "val12").returns(13, null).returns(14, "val14")
            .returns(15, null).returns(16, "val16").returns(17, null).returns(18, "val18").returns(19, null);

        checkRebuildIndexQuery(grid(1), checker, checker);

        // Correlated join with correlation in filter, with project.
        sql = "SELECT /*+ DISABLE_RULE('MergeJoinConverter', 'NestedLoopJoinConverter') */ tbl2.id, tbl.val1 " +
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
