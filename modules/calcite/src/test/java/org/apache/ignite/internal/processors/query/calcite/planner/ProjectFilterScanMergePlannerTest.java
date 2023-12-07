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
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests ProjectScanMergeRule and FilterScanMergeRule.
 */
public class ProjectFilterScanMergePlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
            .add("A", f.createSqlType(SqlTypeName.INTEGER))
            .add("B", f.createSqlType(SqlTypeName.INTEGER))
            .add("C", f.createSqlType(SqlTypeName.INTEGER))
            .build();

        createTable(publicSchema, "TBL", type, IgniteDistributions.single(), null);
    }

    /** */
    @Test
    public void testProjectFilterMerge() throws Exception {
        // Order of merge: ((scan + filter) + project).
        assertPlan("SELECT a, b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteTableScan.class)
            .and(scan -> scan.projects() != null)
            .and(scan -> "[$t0, $t1]".equals(scan.projects().toString()))
            .and(scan -> scan.condition() != null)
            .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
            .and(scan -> ImmutableBitSet.of(0, 1, 2).equals(scan.requiredColumns()))
        );
    }

    /** */
    @Test
    public void testIdentityFilterMerge() throws Exception {
        // Order of merge: ((scan + filter) + identity).
        assertPlan("SELECT a, b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteTableScan.class)
            .and(scan -> scan.projects() == null)
            .and(scan -> scan.condition() != null)
            .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
            .and(scan -> ImmutableBitSet.of(0, 1, 2).equals(scan.requiredColumns()))
        );
    }

    /** */
    @Test
    public void testProjectFilterMergeIndex() throws Exception {
        // Test project and filter merge into index scan.
        TestTable tbl = ((TestTable)publicSchema.getTable("TBL"));
        tbl.addIndex("IDX_C", 2);

        // Without index condition shift.
        assertPlan("SELECT a, b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
            .and(scan -> scan.projects() != null)
            .and(scan -> "[$t0, $t1]".equals(scan.projects().toString()))
            .and(scan -> scan.condition() != null)
            .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
            .and(scan -> ImmutableBitSet.of(0, 1, 2).equals(scan.requiredColumns()))
            .and(scan -> "[=($t2, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );

        // Index condition shifted according to requiredColumns.
        assertPlan("SELECT b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
            .and(scan -> scan.projects() != null)
            .and(scan -> "[$t0]".equals(scan.projects().toString()))
            .and(scan -> scan.condition() != null)
            .and(scan -> "=($t1, 0)".equals(scan.condition().toString()))
            .and(scan -> ImmutableBitSet.of(1, 2).equals(scan.requiredColumns()))
            .and(scan -> "[=($t1, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );
    }

    /** */
    @Test
    public void testIdentityFilterMergeIndex() throws Exception {
        // Test project and filter merge into index scan.
        TestTable tbl = ((TestTable)publicSchema.getTable("TBL"));
        tbl.addIndex("IDX_C", 2);

        // Without index condition shift.
        assertPlan("SELECT a, b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
            .and(scan -> scan.projects() == null)
            .and(scan -> scan.condition() != null)
            .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
            .and(scan -> ImmutableBitSet.of(0, 1, 2).equals(scan.requiredColumns()))
            .and(scan -> "[=($t2, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );

        // Index condition shift and identity.
        assertPlan("SELECT b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
            .and(scan -> scan.projects() == null)
            .and(scan -> scan.condition() != null)
            .and(scan -> "=($t1, 0)".equals(scan.condition().toString()))
            .and(scan -> ImmutableBitSet.of(1, 2).equals(scan.requiredColumns()))
            .and(scan -> "[=($t1, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );
    }

    /** */
    @Test
    public void testProjectFilterProjectMerge() throws Exception {
        // Inner query contains correlate, it prevents filter to be moved below project, and after HEP_FILTER_PUSH_DOWN
        // phase we should have chain Project - Filter - Project - Scan. Whole this chain should be merged into a single
        // table scan on the next phases. Order of merge: (((scan + inner project) + filter) + outer project).
        String sql = "SELECT (SELECT a+2 FROM (SELECT c, a+1 AS a FROM tbl) AS t2 WHERE t2.c = t1.c) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
            .and(input(isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[+(+($t0, 1), 2)]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, $cor0.C)".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0, 2).equals(scan.requiredColumns()))
            ))));
    }

    /** */
    @Test
    public void testIdentityFilterProjectMerge() throws Exception {
        // The same as two projects merge, but outer project is identity and should be eliminated together with inner
        // project by project to scan merge rule.
        String sql = "SELECT (SELECT a FROM (SELECT a, a+1 FROM tbl) AS t2 WHERE t2.a = t1.a) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
            .and(input(isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t0, $cor0.A)".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0).equals(scan.requiredColumns()))
            ))), "ProjectFilterTransposeRule");

        // Filter on project that is not permutation should be merged too.
        sql = "SELECT (SELECT a FROM (SELECT a+1 AS a FROM tbl) AS t2 WHERE t2.a = t1.a) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
            .and(input(isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[+($t0, 1)]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=(+($t0, 1), $cor0.A)".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0).equals(scan.requiredColumns()))
            ))), "ProjectFilterTransposeRule");
    }

    /** */
    @Test
    public void testProjectFilterIdentityMerge() throws Exception {
        // The same as two projects merge, but inner project is identity and should be eliminated by project to scan
        // merge rule.
        String sql = "SELECT (SELECT a+2 FROM (SELECT a, c FROM tbl) AS t2 WHERE t2.c = t1.c) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
            .and(input(isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[+($t0, 2)]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, $cor0.C)".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0, 2).equals(scan.requiredColumns()))
            ))));
    }

    /** */
    @Test
    public void testIdentityFilterIdentityMerge() throws Exception {
        // The same as two projects merge, but projects are identity and should be eliminated by project to scan
        // merge rule.
        String sql = "SELECT (SELECT c FROM (SELECT a AS c FROM tbl) AS t2 WHERE t2.c = t1.c) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
            .and(input(isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t0, $cor0.C)".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0).equals(scan.requiredColumns()))
            ))));
    }

    /** */
    @Test
    public void testFilterProjectFilterMerge() throws Exception {
        String sql = "SELECT * FROM (SELECT c, a FROM tbl WHERE a = 1) WHERE c = 1";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[$t1, $t0]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "AND(=($t0, 1), =($t1, 1))".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0, 2).equals(scan.requiredColumns())),
            "ProjectFilterTransposeRule", "FilterProjectTransposeRule");
    }

    /** */
    @Test
    public void testFilterIdentityFilterMerge() throws Exception {
        String sql = "SELECT * FROM (SELECT a, c FROM tbl WHERE a = 1) WHERE c = 1";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "AND(=($t0, 1), =($t1, 1))".equals(scan.condition().toString()))
                .and(scan -> ImmutableBitSet.of(0, 2).equals(scan.requiredColumns())),
            "ProjectFilterTransposeRule", "FilterProjectTransposeRule");
    }

    /** */
    private static List<RexNode> searchBoundsCondition(List<SearchBounds> searchBounds) {
        return searchBounds.stream().filter(Objects::nonNull).map(SearchBounds::condition).collect(Collectors.toList());
    }
}
