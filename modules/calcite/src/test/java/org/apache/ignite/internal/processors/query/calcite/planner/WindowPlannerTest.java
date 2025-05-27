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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.junit.Before;
import org.junit.Test;

import static java.util.function.Predicate.not;

/**
 *
 */
public class WindowPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    private final IgniteDistribution affinity = IgniteDistributions.affinity(0, "affinity_tbl", "hash");
    private final IgniteDistribution hash = IgniteDistributions.hash(ImmutableIntList.of(0));

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();
        publicSchema = createSchema(
            createTable("RANDOM_TBL", IgniteDistributions.random(),
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER),
            createTable("SINGLE_TBL", IgniteDistributions.single(),
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER),
            createTable("AFFINITY_TBL", affinity,
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER)
                .addIndex("AFFINITY_TBL_IDX", 0),
            createTable("HASH_TBL", hash,
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER)
        );
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testSplitWindowRelsByGroup() throws Exception {
        String sql = "SELECT SUM(VALUE) OVER (PARTITION BY ID), ROW_NUMBER() OVER () FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(IgniteWindow::isStreaming)
                .and(it -> "window(rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])".equals(it.getGroup().toString()))
                .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                    .and(not(IgniteWindow::isStreaming))
                    .and(it -> "window(partition {0} aggs [SUM($1)])".equals(it.getGroup().toString()))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testProjectWindowConstants() throws Exception {
        String sql = "SELECT ID, VALUE, MAX(2) OVER (PARTITION BY ID ORDER BY VALUE ROWS BETWEEN 10 PRECEDING AND 20 FOLLOWING) FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            isInstanceOf(IgniteProject.class)
                // the top project should remove constants from the window rel on position 2..4.
                .and(project -> "[$0, $1, $5]".equals(project.getProjects().toString()))
                .and(input(isInstanceOf(IgniteWindow.class)
                    .and(it -> it.getGroup().lowerBound.getOffset() instanceof RexLiteral
                        && it.getGroup().upperBound.getOffset() instanceof RexLiteral)
                    .and(hasChildThat(isInstanceOf(IgniteProject.class)
                        // the bottom project should add agg call constants from the window rel on position 2..4
                        .and(project -> "[$0, $1, 2, 10, 20]".equals(project.getProjects().toString())))))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule", "ProjectMergeRule", "ProjectWindowTransposeRule");
    }


    /**
     * @throws Exception if failed
     */
    @Test
    public void testProjectWindowTranspose() throws Exception {
        String sql = "SELECT MAX(VALUE) OVER (ORDER BY VALUE ROWS BETWEEN VALUE PRECEDING AND 10 FOLLOWING) FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasChildThat(isInstanceOf(IgniteProject.class)
                    // ProjectWindowTransposeRule should eliminate constant (10) in projection
                    .and(project -> project.getProjects().stream().noneMatch(it -> it instanceof RexLiteral))))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testOrderByWithWindow() throws Exception {
        String sql = "SELECT MAX(VALUE) OVER (PARTITION BY ID) FROM RANDOM_TBL ORDER BY ID DESC, VALUE";

        RelCollation derivedCollation = RelCollations.of(
            TraitUtils.createFieldCollation(0, false),
            TraitUtils.createFieldCollation(1, true)
        );
        assertPlan(sql, publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
                .and(window -> window.collation().equals(derivedCollation))
                .and(input(isInstanceOf(IgniteExchange.class)))
                .and(exchange -> exchange.collation().equals(derivedCollation))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashTableAndWindow() throws Exception {
        String sql = "SELECT FIRST_VALUE(VALUE) OVER (PARTITION BY ID) FROM HASH_TBL";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(hash)))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashTableWithAnotherDistAndWindow() throws Exception {
        String sql = "SELECT FIRST_VALUE(VALUE) OVER (PARTITION BY VALUE) FROM HASH_TBL";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteWindow.class)
            .and(hasDistribution(IgniteDistributions.single())))
            .and(hasChildThat(isInstanceOf(IgniteExchange.class))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testIndexedAffinityTableAndWindow() throws Exception {
        String sql = "SELECT MAX(VALUE) OVER (PARTITION BY ID) FROM (SELECT * FROM AFFINITY_TBL) S";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(affinity))
                .and(input(isIndexScan("AFFINITY_TBL", "AFFINITY_TBL_IDX")))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testCorrelatedSubqueryWithWindow() throws Exception {
        String sql = "SELECT ID, (SELECT row_number() OVER (ORDER BY ID)) FROM RANDOM_TBL i1 ORDER BY ID";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, nodeOrAnyChild(isTableScan("random_tbl"))))
            .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteWindow.class))))));
    }
}
