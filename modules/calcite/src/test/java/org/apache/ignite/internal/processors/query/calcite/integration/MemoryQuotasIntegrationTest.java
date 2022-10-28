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

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class MemoryQuotasIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final long GLOBAL_MEM_QUOTA = 10_000_000L;

    /** */
    private static final long QRY_MEMORY_QUOTA = 1_000_000L;

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()
                .setGlobalMemoryQuota(GLOBAL_MEM_QUOTA).setQueryMemoryQuota(QRY_MEMORY_QUOTA)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE tbl (id INT, b VARBINARY) WITH TEMPLATE=REPLICATED");

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO tbl VALUES (?, ?)", i, new byte[1000]);
    }

    /** */
    @Test
    public void testSortNode() {
        assertQuery("SELECT id, b FROM tbl WHERE id < 800 ORDER BY id")
            .matches(QueryChecker.containsSubPlan("IgniteSort"))
            .resultSize(800)
            .check();

        assertQuery("SELECT id, b FROM tbl ORDER BY id LIMIT 800")
            .matches(QueryChecker.containsSubPlan("IgniteSort"))
            .resultSize(800)
            .check();

        assertThrows("SELECT id, b FROM tbl ORDER BY id", IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testCollectNode() {
        assertQuery("SELECT MAP(SELECT id, b FROM tbl WHERE id < 800)")
            .matches(QueryChecker.containsSubPlan("IgniteCollect"))
            .resultSize(1)
            .check();

        assertThrows("SELECT MAP(SELECT id, b FROM tbl)", IgniteException.class, "Query quota exceeded");

        assertQuery("SELECT ARRAY(SELECT b FROM tbl WHERE id < 800)")
            .matches(QueryChecker.containsSubPlan("IgniteCollect"))
            .resultSize(1)
            .check();

        assertThrows("SELECT ARRAY(SELECT b FROM tbl)", IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testMinusNode() {
        // Colocated.
        assertQuery("SELECT id, b FROM tbl WHERE id < 800 EXCEPT (SELECT 0, x'00')")
            .matches(QueryChecker.containsSubPlan("IgniteColocatedMinus"))
            .resultSize(800)
            .check();

        assertThrows("SELECT id, b FROM tbl EXCEPT (SELECT 0, x'00')",
            IgniteException.class, "Query quota exceeded");

        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        sql("CREATE TABLE tbl3 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO tbl3 VALUES (?, ?)", i, new byte[1000]);

        assertQuery("SELECT /*+ DISABLE_RULE('ColocatedMinusConverterRule') */ * FROM " +
            "(SELECT id, b FROM tbl2 EXCEPT SELECT id, b FROM tbl3 WHERE id < 800)")
            .matches(QueryChecker.containsSubPlan("IgniteMapMinus"))
            .resultSize(200)
            .check();

        // On map phase.
        assertThrows("SELECT /*+ DISABLE_RULE('ColocatedMinusConverterRule') */ * FROM " +
            "(SELECT id, b FROM tbl2 EXCEPT SELECT id+1000, b FROM tbl3)",
            IgniteException.class, "Query quota exceeded");

        // On reduce phase.
        assertThrows("SELECT /*+ DISABLE_RULE('ColocatedMinusConverterRule') */ * FROM " +
                "(SELECT id, b FROM tbl2 EXCEPT SELECT 0, x'00')",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testIntersectNode() {
        // Colocated.
        assertQuery("SELECT id, b FROM tbl WHERE id < 800 INTERSECT SELECT id, b FROM tbl")
            .matches(QueryChecker.containsSubPlan("IgniteColocatedIntersect"))
            .resultSize(800)
            .check();

        assertThrows("SELECT id, b FROM tbl INTERSECT (SELECT 0, x'00')",
            IgniteException.class, "Query quota exceeded");

        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 2000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        sql("CREATE TABLE tbl3 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO tbl3 VALUES (?, ?)", i, new byte[1000]);

        assertQuery("SELECT /*+ DISABLE_RULE('ColocatedIntersectConverterRule') */ * FROM " +
            "(SELECT id, b FROM tbl2 WHERE id < 800 INTERSECT SELECT id, b FROM tbl3 WHERE id < 800)")
            .matches(QueryChecker.containsSubPlan("IgniteMapIntersect"))
            .resultSize(800)
            .check();

        // On map phase.
        assertThrows("SELECT /*+ DISABLE_RULE('ColocatedIntersectConverterRule') */ * FROM " +
                "(SELECT id, b FROM tbl2 INTERSECT SELECT 0, x'00')",
            IgniteException.class, "Query quota exceeded");

        // On reduce phase.
        assertThrows("SELECT /*+ DISABLE_RULE('ColocatedIntersectConverterRule') */ * FROM " +
                "(SELECT id, b FROM tbl2 WHERE id < 1000 INTERSECT SELECT 0, x'00')",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testHashSpoolNode() {
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 800; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertQuery("SELECT /*+ DISABLE_RULE('FilterSpoolMergeToSortedIndexSpoolRule') */ " +
            "(SELECT b FROM tbl2 WHERE tbl2.id = tbl.id) FROM tbl")
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .resultSize(1000)
            .check();

        for (int i = 800; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertThrows("SELECT /*+ DISABLE_RULE('FilterSpoolMergeToSortedIndexSpoolRule') */ " +
                "(SELECT b FROM tbl2 WHERE tbl2.id = tbl.id) FROM tbl",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testSortedSpoolNode() {
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY, PRIMARY KEY(id)) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 800; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertQuery("SELECT /*+ DISABLE_RULE('FilterSpoolMergeToHashIndexSpoolRule') */ " +
            "(SELECT b FROM tbl2 WHERE tbl2.id = tbl.id) FROM tbl")
            .matches(QueryChecker.containsSubPlan("IgniteSortedIndexSpool"))
            .resultSize(1000)
            .check();

        for (int i = 800; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertThrows("SELECT /*+ DISABLE_RULE('FilterSpoolMergeToHashIndexSpoolRule') */ " +
                "(SELECT b FROM tbl2 WHERE tbl2.id = tbl.id) FROM tbl",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testTableSpoolNode() {
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 800; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertQuery("SELECT (SELECT b FROM tbl2 WHERE tbl2.id + 1 = tbl.id + 1) FROM tbl")
            .matches(QueryChecker.containsSubPlan("IgniteTableSpool"))
            .resultSize(1000)
            .check();

        for (int i = 800; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertThrows("SELECT (SELECT b FROM tbl2 WHERE tbl2.id = tbl.id) FROM tbl",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testNestedLoopJoinNode() {
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 800; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertQuery("SELECT /*+ DISABLE_RULE('CorrelatedNestedLoopJoin', 'MergeJoinConverter') */ " +
            "tbl.id, tbl.b, tbl2.id, tbl2.b FROM tbl JOIN tbl2 USING (id)")
            .matches(QueryChecker.containsSubPlan("IgniteNestedLoopJoin"))
            .resultSize(800)
            .check();

        for (int i = 800; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertThrows("SELECT /*+ DISABLE_RULE('CorrelatedNestedLoopJoin', 'MergeJoinConverter') */" +
                "tbl.id, tbl.b, tbl2.id, tbl2.b FROM tbl JOIN tbl2 USING (id)",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testSortAggregateNode() {
        // Colocated.
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=REPLICATED");

        sql("CREATE INDEX idx2 ON tbl2(id)");

        for (int i = 0; i < 800; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", 0, new byte[1000]);

        assertQuery("SELECT ARRAY_AGG(b) FROM tbl2 GROUP BY id")
            .matches(QueryChecker.containsSubPlan("IgniteColocatedSortAggregate"))
            .resultSize(1)
            .check();

        for (int i = 800; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", 0, new byte[1000]);

        assertThrows("SELECT ARRAY_AGG(b) FROM tbl2 GROUP BY id",
            IgniteException.class, "Query quota exceeded");

        // Map-reduce.
        sql("CREATE TABLE tbl3 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        sql("CREATE INDEX idx3 ON tbl3(id)");

        for (int i = 0; i < 800; i++)
            sql("INSERT INTO tbl3 VALUES (?, ?)", 0, new byte[1000]);

        assertQuery("SELECT ARRAY_AGG(b) FROM tbl3 GROUP BY id")
            .matches(QueryChecker.containsSubPlan("IgniteMapSortAggregate"))
            .resultSize(1)
            .check();

        for (int i = 800; i < 1000; i++)
            sql("INSERT INTO tbl3 VALUES (?, ?)", 0, new byte[1000]);

        // Reduce phase.
        assertThrows("SELECT ARRAY_AGG(b) FROM tbl3 GROUP BY id",
            IgniteException.class, "Query quota exceeded");

        for (int i = 1000; i < 2000; i++)
            sql("INSERT INTO tbl3 VALUES (?, ?)", 0, new byte[1000]);

        // Map phase.
        assertThrows("SELECT ARRAY_AGG(b) FROM tbl3 GROUP BY id",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testHashAggregateNode() {
        // Colocated hash aggregate.
        assertQuery("SELECT ANY_VALUE(b) FROM tbl WHERE id < 800 GROUP BY id")
            .matches(QueryChecker.containsSubPlan("IgniteColocatedHashAggregate"))
            .resultSize(800)
            .check();

        assertThrows("SELECT ANY_VALUE(b) FROM tbl GROUP BY id",
            IgniteException.class, "Query quota exceeded");

        //  Colocated AggAccumulator.
        assertQuery("SELECT ARRAY_AGG(b) FROM tbl WHERE id < 800")
            .matches(QueryChecker.containsSubPlan("IgniteColocatedHashAggregate"))
            .resultSize(1)
            .check();

        assertThrows("SELECT ARRAY_AGG(b) FROM tbl",
            IgniteException.class, "Query quota exceeded");

        // Map-reduce.
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=PARTITIONED");

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        // Reduce phase hash aggregate.
        assertQuery("SELECT ANY_VALUE(b) FROM tbl2 WHERE id < 600 GROUP BY id")
            .matches(QueryChecker.containsSubPlan("IgniteReduceHashAggregate"))
            .resultSize(600)
            .check();

        assertThrows("SELECT ANY_VALUE(b) FROM tbl2 GROUP BY id",
            IgniteException.class, "Query quota exceeded");

        // Reduce phase AggAccumulator.
        assertQuery("SELECT ARRAY_AGG(b) FROM tbl2 WHERE id < 800")
            .matches(QueryChecker.containsSubPlan("IgniteReduceHashAggregate"))
            .resultSize(1)
            .check();

        assertThrows("SELECT ARRAY_AGG(b) FROM tbl2",
            IgniteException.class, "Query quota exceeded");

        // Map phase.
        for (int i = 1000; i < 2000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[1000]);

        assertThrows("SELECT ANY_VALUE(b) FROM tbl2 GROUP BY id",
            IgniteException.class, "Query quota exceeded");

        assertThrows("SELECT ARRAY_AGG(b) FROM tbl2",
            IgniteException.class, "Query quota exceeded");
    }

    /** */
    @Test
    public void testGlobalQuota() {
        sql("CREATE TABLE tbl2 (id INT, b VARBINARY) WITH TEMPLATE=REPLICATED");

        for (int i = 0; i < 3_000; i++)
            sql("INSERT INTO tbl2 VALUES (?, ?)", i, new byte[200]);

        QueryCursor<?>[] curs = new QueryCursor[20];

        String sql = "SELECT id, b FROM tbl2 ORDER BY id";

        try {
            for (int i = 0; i < 10; i++) {
                curs[i] = queryProcessor(grid(0)).query(null, "PUBLIC", sql).get(0);
                curs[i].iterator().next(); // Start fetching.
            }

            GridTestUtils.assertThrowsAnyCause(log, () -> {
                for (int i = 10; i < 20; i++) {
                    curs[i] = queryProcessor(grid(0)).query(null, "PUBLIC", sql).get(0);
                    curs[i].iterator().next();
                }
                return null;
            }, IgniteException.class, "Global memory quota for SQL queries exceeded");
        }
        finally {
            for (int i = 0; i < 20; i++) {
                if (curs[i] != null)
                    curs[i].close();
            }
        }
    }
}
