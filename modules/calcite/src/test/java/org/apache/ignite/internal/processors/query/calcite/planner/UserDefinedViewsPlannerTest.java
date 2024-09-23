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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/**
 * User defined views test.
 */
public class UserDefinedViewsPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleView() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "C1", INTEGER, "C2", INTEGER, "C3", INTEGER),
            createTable("T2", IgniteDistributions.single(), "C1", INTEGER, "C2", INTEGER, "C3", INTEGER)
        );

        String viewSql = "SELECT T1.C1 AS C1_1, T1.C2 AS C1_2, T2.C1 AS C2_1, T2.C2 AS C2_2 FROM T1 JOIN T2 ON (T1.C3 = T2.C3)";

        schema.addView("V1", viewSql);

        String sql = "select * from v1 where c1_1 = 1 AND c2_2 = 2";

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(hasChildThat(isTableScan("T1")
                .and(t -> "=($t0, 1)".equals(t.condition().toString()))))
            .and(hasChildThat(isTableScan("T2")
                .and(t -> "=($t1, 2)".equals(t.condition().toString()))))
        ));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testViewOnView() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "C1", INTEGER, "C2", INTEGER, "C3", INTEGER)
        );

        schema.addView("V1", "SELECT C1, C2 FROM T1");
        schema.addView("V2", "SELECT C1 FROM V1");

        assertPlan("select * from v2", schema, isTableScan("T1")
            .and(t -> t.requiredColumns().equals(ImmutableBitSet.of(0))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testViewHint() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "C1", INTEGER, "C2", INTEGER, "C3", INTEGER)
                .addIndex("IDX_1", 0)
                .addIndex("IDX_2", 1)
        );

        schema.addView("V1", "SELECT /*+ FORCE_INDEX(IDX_1) */ C1, C2 FROM T1");
        schema.addView("V2", "SELECT /*+ FORCE_INDEX(IDX_2) */ C1, C2 FROM T1");

        assertPlan("SELECT * FROM v1 WHERE c1 = ? AND c2 = ?", schema, isIndexScan("T1", "IDX_1"));
        assertPlan("SELECT * FROM v2 WHERE c1 = ? AND c2 = ?", schema, isIndexScan("T1", "IDX_2"));
    }

    /** */
    @Test
    public void testRecursiveView() {
        IgniteSchema schema = createSchema();

        schema.addView("V1", "SELECT * FROM V1");

        GridTestUtils.assertThrowsAnyCause(log, () -> physicalPlan("select * from v1", schema), IgniteSQLException.class,
            "Recursive views are not supported");

        schema.addView("V2", "SELECT * FROM V3");
        schema.addView("V3", "SELECT * FROM V2");

        GridTestUtils.assertThrowsAnyCause(log, () -> physicalPlan("select * from v2", schema), IgniteSQLException.class,
            "Recursive views are not supported");
    }
}
