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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.List;
import java.util.function.Consumer;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdRowCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.generateProxyIdxName;

/**
 * Tests estimation of rows number of a inner join production.
 *
 * @see IgniteMdRowCount#joinRowCount(RelMetadataQuery, Join)
 */
public class InnerJoinRowsEstimationTest extends AbstractPlannerTest {
    /** */
    private static final int LEFT_TBL_SZ = 100;

    /** */
    private static final int RIGHT_TBL_SZ = 1000;

    /** Proxy PK on PK. The rows number should be taken from the left table. */
    @Test
    public void testProxyPkOnPk() throws Exception {
        toTest(
            t -> t.addIndex(generateProxyIdxName(QueryUtils.PRIMARY_KEY_INDEX), 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.id=r.id",
            rowsNumberIs(LEFT_TBL_SZ),
            joinChecker()
        );
    }

    /** Entire composite PK on entire composite PK. The rows number should be taken from the left table. */
    @Test
    public void testCompositePkOnCompositePk() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 1, 0),
            "l.id=r.val0 and l.val0=r.id",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(2, true)
        );
    }
    
    /**
     * Entire composite PK on entire composite PK with additional condition.
     * The rows number should be fewer than the left table size.
     */
    @Test
    public void testCompositePkOnCompositePkAdditionalCondition() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 1, 0),
            "l.id=r.val0 and l.val0=r.id and l.val1=r.val1",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(3, true)
        );
    }

    /** Composite PK leading columns. The rows number should be taken from the left table. */
    @Test
    public void testCompositePkLeadings() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 1, 0),
            "l.id=r.val0",
            rowsNumberIs(LEFT_TBL_SZ),
            joinChecker()
        );
    }

    /** 
     * Composite PKs' leading columns with additional non-equi condition.
     * The rows number should be fewer than the left table size. 
     */
    @Test
    public void testCompositePkLeadingsWithNonEqui() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 1, 0),
            "l.id=r.val0 and l.val1>r.val1",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(1, false)
        );
    }

    /** Composite PK leading column on composite PK value. The rows number should be taken from the right table. */
    @Test
    public void testCompositePkLeadingOnCompositePkValue() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 1, 0),
            "l.id=r.id",
            rowsNumberIs(RIGHT_TBL_SZ),
            joinChecker()
        );
    }

    /**
     * Composite PK leading column on composite PK value with additional non-equi condition.
     * The rows number should be fewer than the right table size.
     */
    @Test
    public void testCompositePkLeadingOnCompositePkValueWithNonEqui() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 1, 0),
            "l.id=r.id and l.val1>r.val1",
            rowsNumberBetween(LEFT_TBL_SZ, RIGHT_TBL_SZ),
            joinChecker(1, false)
        );
    }

    /** Proxy PK on proxy PK. The rows number should be taken from the left table. */
    @Test
    public void testPkOnProxy() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(generateProxyIdxName(QueryUtils.PRIMARY_KEY_INDEX), 0),
            "l.id=r.id",
            rowsNumberIs(LEFT_TBL_SZ),
            joinChecker()
        );
    }

    /** PK on composite PK. The rows number should be taken from the smaller table. */
    @Test
    public void testPkOnCompositePk() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            "l.id=r.id and r.val0 = l.val0",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(2, true)
        );
    }

    /** PK on composite PK value. The rows number should be taken from the right table. */
    @Test
    public void testPkOnCompositePkValue() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0, 1),
            "l.id=r.val0",
            rowsNumberIs(RIGHT_TBL_SZ),
            joinChecker()
        );
    }

    /** Proxy PK on PK join with additional condition. The rows number should be fewer that the left table size. */
    @Test
    public void testPkOnPkWithAdditionalCondition() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(generateProxyIdxName(QueryUtils.PRIMARY_KEY_INDEX), 0),
            "l.id=r.id and l.val0=r.val1",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(2, true)
        );
    }

    /** Proxy PK on PK join with additional non-equi conditions. The rows number should be fewer that the left table size. */
    @Test
    public void testPkOnPkWithNonEqui() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(generateProxyIdxName(QueryUtils.PRIMARY_KEY_INDEX), 0),
            "l.id=r.id and l.val0>r.val1",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(1, false)
        );
    }

    /** A value on PK join. The rows number should be taken from the left table. */
    @Test
    public void testValueOnPk() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.val0=r.id",
            rowsNumberIs(LEFT_TBL_SZ),
            joinChecker()
        );
    }

    /** A value on PK join with additional condition. The rows number should be fewer that the left table size. */
    @Test
    public void testValueOnPkWithAdjustment() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.val0=r.id and l.val1=r.val0",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(2, true)
        );
    }

    /** A value on PK join with additional non-equi condition. The rows number should be fewer that the left table size. */
    @Test
    public void testValueOnPkWithNonEqui() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.val0=r.id and l.val1>r.val0",
            rowsNumberLess(LEFT_TBL_SZ),
            joinChecker(1, false)
        );
    }

    /** PK on value. The rows number should be taken from the right table. */
    @Test
    public void testPkOnValue() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.id=r.val0",
            rowsNumberIs(RIGHT_TBL_SZ),
            joinChecker()
        );
    }

    /** PK on value join with additional condition. The rows number should be fewer than the right table size. */
    @Test
    public void testPkOnValueWithAdditionalCondition() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.id=r.val0 and l.val0=r.val1",
            rowsNumberBetween(LEFT_TBL_SZ, RIGHT_TBL_SZ),
            joinChecker(2, true)
        );
    }

    /** PK on value join with additional non-equi condition. The rows number should be fewer than the right table size. */
    @Test
    public void testPkOnValueWithNonEqui() throws Exception {
        toTest(
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            t -> t.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0),
            "l.id=r.val0 and l.val0>r.val1",
            rowsNumberBetween(LEFT_TBL_SZ, RIGHT_TBL_SZ),
            joinChecker(1, false)
        );
    }

    /** */
    protected void toTest(
        Consumer<TestTable> leftTblFixer,
        Consumer<TestTable> rightTblFixer,
        String joinCondition,
        Consumer<Double> rowsNumberChecker,
        @Nullable Consumer<Join> joinChecker
    ) throws Exception {
        TestTable tbl0 = createTable("T0", LEFT_TBL_SZ, IgniteDistributions.affinity(0, "test", "hash"),
            "ID", Integer.class, "VAL0", Integer.class, "VAL1", Integer.class);
        leftTblFixer.accept(tbl0);

        TestTable tbl1 = createTable("T1", RIGHT_TBL_SZ, IgniteDistributions.affinity(0, "test", "hash"),
            "ID", Integer.class, "VAL0", Integer.class, "VAL1", Integer.class);
        rightTblFixer.accept(tbl1);

        IgniteSchema schema = createSchema(tbl0, tbl1);

        String sql = "select /*+ ENFORCE_JOIN_ORDER */ l.val1, r.val0 from T0 l join T1 r on " + joinCondition;

        if (log.isInfoEnabled())
            log.info("Test query: " + sql);

        IgniteRel root = physicalPlan(sql, schema);

        Join join = findFirstNode(root, byClass(Join.class));

        if (joinChecker != null)
            joinChecker.accept(join);

        RelMetadataQuery mq = join.getCluster().getMetadataQuery();

        rowsNumberChecker.accept(IgniteMdRowCount.joinRowCount(mq, join));
    }

    /** */
    protected Consumer<Double> rowsNumberIs(int cnt) {
        return d -> assertEquals(d.intValue(), cnt);
    }

    /** */
    protected Consumer<Double> rowsNumberLess(int cnt) {
        return d -> assertTrue(d.intValue() < cnt);
    }

    /** */
    protected Consumer<Double> rowsNumberBetween(int min, int max) {
        return d -> assertTrue(d.intValue() > min && d.intValue() < max);
    }

    /** */
    protected static Consumer<Join> joinChecker() {
        return joinChecker(1, true);
    }

    /** */
    protected static Consumer<Join> joinChecker(int pairsCnt, boolean isEqui) {
        return join -> {
            // This test is for an inner join by now.
            assertEquals(join.getJoinType(), JoinRelType.INNER);

            JoinInfo joinInfo = join.analyzeCondition();
            List<IntPair> pairs = joinInfo.pairs();

            assertEquals(pairsCnt, pairs.size());

            assertTrue(isEqui == joinInfo.isEqui());
            assertTrue(isEqui == joinInfo.nonEquiConditions.isEmpty());
        };
    }
}
