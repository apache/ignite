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
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/**
 * Tests for Rex simplification.
 */
public class RexSimplificationPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExtractCommonDisjunctionPart() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "C1", INTEGER, "C2", INTEGER, "C3", INTEGER)
                .addIndex("IDX1", 0),
            createTable("T2", IgniteDistributions.single(), "C1", INTEGER, "C2", INTEGER, "C3", INTEGER)
                .addIndex("IDX2", 0)
        );

        // Simple equality predicate.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 1) or " +
                "(c1 = 0 and c3 = 2)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), OR(=($t1, 1), =($t2, 2)))".equals(s.condition().toString())));

        // Reversed equality predicate.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 1) or " +
                "(0 = c1 and c3 = 2)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), OR(=($t1, 1), =($t2, 2)))".equals(s.condition().toString())));

        // Simple more-than predicate.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 > 0 and c2 = 1) or " +
                "(c1 > 0 and c3 = 2)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(>($t0, 0), OR(=($t1, 1), =($t2, 2)))".equals(s.condition().toString())));

        // Reversed more-than predicate.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 > 0 and c2 = 1) or " +
                "(0 < c1 and c3 = 2)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(>($t0, 0), OR(=($t1, 1), =($t2, 2)))".equals(s.condition().toString())));

        // Three operands disjunction.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 1) or " +
                "(c1 = 0 and c3 = 2) or " +
                "(c1 = 0 and c2 = c3)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), OR(=($t1, 1), =($t2, 2), =($t1, $t2)))".equals(s.condition().toString())));

        // Two operands extraction.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 0 and c3 = 0) or " +
                "(c1 = 0 and c2 = 0 and c3 = 1) or " +
                "(c1 = 0 and c2 = 0 and c3 = 2)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), =($t1, 0), SEARCH($t2, Sarg[0, 1, 2]))".equals(s.condition().toString())));

        // Disjunction equality first operand removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0) or " +
                "(c1 = 0 and c2 = 0) or " +
                "(c1 = 0 and c2 = 1)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), SEARCH($t1, Sarg[0, 1]))".equals(s.condition().toString())));

        // Disjunction equality last operand removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 0) or " +
                "(c1 = 0 and c2 = 1) or " +
                "(c1 = 0)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), SEARCH($t1, Sarg[0, 1]))".equals(s.condition().toString())));

        // Disjunction conjunction operand removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 0 and c3 = 0) or " +
                "(c1 = 0 and c2 = 0 and c3 = 1) or " +
                "(c1 = 0 and c2 = 0)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), =($t1, 0), SEARCH($t2, Sarg[0, 1]))".equals(s.condition().toString())));

        // Disjunction removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0) or " +
                "(c1 = 0)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "=($t0, 0)".equals(s.condition().toString())));

        // Disjunction removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0) or " +
                "(c1 = 0 and c2 = 0)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), =($t1, 0))".equals(s.condition().toString())));

        // Disjunction removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 0) or " +
                "(c1 = 0)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), =($t1, 0))".equals(s.condition().toString())));

        // Disjunction removal.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 0) or " +
                "(c2 = 0 and c1 = 0)", schema,
            isIndexScan("T1", "IDX1")
                .and(s -> "AND(=($t0, 0), =($t1, 0))".equals(s.condition().toString())));

        // Simple join.
        assertPlan("SELECT * FROM t1 JOIN t2 ON (" +
                "(t1.c1 = t2.c1 and t1.c2 = 0) or " +
                "(t1.c1 = t2.c1 and t2.c2 = 0))", schema,
            isInstanceOf(Join.class)
                .and(s -> "AND(=($0, $3), OR(=($1, 0), =($4, 0)))".equals(s.getCondition().toString())));

        // Simple join reversed equality predicate.
        assertPlan("SELECT * FROM t1 JOIN t2 ON (" +
                "(t1.c1 = t2.c1 and t1.c2 = 0) or " +
                "(t2.c1 = t1.c1 and t2.c2 = 0))", schema,
            isInstanceOf(Join.class)
                .and(s -> "AND(=($0, $3), OR(=($1, 0), =($4, 0)))".equals(s.getCondition().toString())));

        // Join with filter push-down.
        assertPlan("SELECT * FROM t1 JOIN t2 ON (" +
                "(t1.c1 = t2.c1 and t1.c2 = 0 and t2.c2 = 0 and t1.c3 = 0) or " +
                "(t1.c1 = t2.c1 and t1.c2 = 0 and t2.c2 = 0 and t2.c3 = 0))", schema,
            isInstanceOf(Join.class)
                .and(s -> "AND(=($0, $3), OR(=($2, 0), =($5, 0)))".equals(s.getCondition().toString()))
                .and(hasChildThat(isInstanceOf(ProjectableFilterableTableScan.class)
                    .and(s -> "T1".equals(Util.last(s.getTable().getQualifiedName())))
                    .and(s -> "=($t1, 0)".equals(s.condition().toString()))
                ))
                .and(hasChildThat(isInstanceOf(ProjectableFilterableTableScan.class)
                    .and(s -> "T2".equals(Util.last(s.getTable().getQualifiedName())))
                    .and(s -> "=($t1, 0)".equals(s.condition().toString()))
                ))
        );

        // Can't simplify.
        assertPlan("SELECT * FROM t1 WHERE " +
                "(c1 = 0 and c2 = 1) or " +
                "(c1 = 0 and c3 = 2) or " +
                "(c1 = 1 and c2 = c3)", schema,
            isInstanceOf(ProjectableFilterableTableScan.class)
                .and(s -> "OR(AND(=($t0, 0), =($t1, 1)), AND(=($t0, 0), =($t2, 2)), AND(=($t0, 1), =($t1, $t2)))"
                    .equals(s.condition().toString()))
        );
    }
}
