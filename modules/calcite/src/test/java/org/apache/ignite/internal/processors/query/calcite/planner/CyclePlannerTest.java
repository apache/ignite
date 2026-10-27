/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Arrays;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRecursiveTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRepeatUnion;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** CYCLE validation and generated path types, following CALCITE-7814. */
public class CyclePlannerTest extends AbstractPlannerTest {
    /** */
    private static final String NUMBERS = "WITH RECURSIVE t(n) AS (VALUES (1) " +
        "UNION ALL SELECT n + 1 FROM t WHERE n < 3) ";

    /** CYCLE lowers to the existing recursive operators and survives physical plan serialization. */
    @Test
    public void testCyclePlan() throws Exception {
        assertPlan(NUMBERS + "CYCLE n SET c USING p SELECT * FROM t", new IgniteSchema(DEFAULT_SCHEMA),
            isInstanceOf(IgniteRepeatUnion.class).and(hasChildThat(isInstanceOf(IgniteRecursiveTableScan.class))));
    }

    /** Key order and quoted field names are retained in the nested ROW type of the path. */
    @ParameterizedTest
    @ValueSource(strings = {"*", "t.*"})
    public void testPathType(String projection) throws Exception {
        String sql = "WITH RECURSIVE t(\"nodeId\", k) AS (VALUES (1, 2) UNION ALL SELECT " +
            projection + " FROM t) CYCLE k, \"nodeId\" SET c USING p SELECT * FROM t";

        try (IgnitePlanner planner = plannerCtx(sql, new IgniteSchema(DEFAULT_SCHEMA)).planner()) {
            RelDataType rowType = planner.validateAndGetType(planner.parse(sql)).right;

            assertEquals(Arrays.asList("nodeId", "K", "C", "P"), rowType.getFieldNames());
            assertEquals(SqlTypeName.BOOLEAN, rowType.getFieldList().get(2).getType().getSqlTypeName());

            RelDataType pathType = rowType.getFieldList().get(3).getType();

            assertEquals(SqlTypeName.ARRAY, pathType.getSqlTypeName());
            assertEquals(Arrays.asList("K", "nodeId"), pathType.getComponentType().getFieldNames());
            assertEquals(SqlTypeName.INTEGER, pathType.getComponentType().getFieldList().get(0).getType().getSqlTypeName());
        }
    }

    /** Invalid key columns and generated column collisions are rejected. */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "CYCLE missing SET c USING p|CYCLE column 'MISSING' is not a column of WITH item 'T'",
        "CYCLE n, n SET c USING p|Duplicate name 'N' in column list",
        "CYCLE n SET n USING p|CYCLE generated column 'N' conflicts with another column",
        "CYCLE n SET c USING n|CYCLE generated column 'N' conflicts with another column",
        "CYCLE n SET c USING c|CYCLE generated column 'C' conflicts with another column"
    })
    public void testInvalidColumns(String clause, String error) throws Exception {
        assertInvalid(NUMBERS + clause + " SELECT * FROM t", error);
    }

    /** The recursive step cannot aggregate or remove duplicates before path construction. */
    @ParameterizedTest
    @ValueSource(strings = {"SELECT DISTINCT n FROM t", "SELECT MAX(n) FROM t", "SELECT SUM(n) OVER () FROM t"})
    public void testInvalidRecursiveSelect(String step) throws Exception {
        assertInvalid("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL " + step + ") " +
            "CYCLE n SET c USING p SELECT * FROM t",
            "CYCLE is not supported with aggregation, DISTINCT, or window functions in the recursive SELECT");
    }

    /** Exactly one direct reference on a non-null-generating side is required. */
    @ParameterizedTest
    @ValueSource(strings = {"SELECT a.n FROM t a JOIN t b ON a.n = b.n",
        "SELECT a.n FROM (VALUES (1)) v(n) LEFT JOIN t a ON a.n = v.n", "SELECT 2"})
    public void testInvalidReference(String step) throws Exception {
        assertInvalid("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL " + step + ") " +
            "CYCLE n SET c USING p SELECT * FROM t",
            "CYCLE requires a UNION [ALL] with a recursive SELECT containing exactly one direct reference to its WITH item");
    }

    /** Explicit RECURSIVE is required by CYCLE even though Ignite can infer it for ordinary recursive CTEs. */
    @Test
    public void testRequiresRecursive() throws Exception {
        assertInvalid(NUMBERS.replace("WITH RECURSIVE", "WITH") + "CYCLE n SET c USING p SELECT * FROM t",
            "CYCLE requires WITH RECURSIVE");
    }

    /** A non-recursive definition cannot acquire recursion merely by declaring CYCLE. */
    @Test
    public void testRequiresRecursiveUnion() throws Exception {
        assertInvalid("WITH RECURSIVE t(n) AS (VALUES (1)) CYCLE n SET c USING p SELECT * FROM t",
            "CYCLE requires a UNION [ALL] with a recursive SELECT containing exactly one direct reference to its WITH item");
    }

    /** Ignite's restriction on user-written nested recursive references remains in force. */
    @Test
    public void testNestedRecursiveReference() throws Exception {
        assertInvalid("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT a.n FROM t a " +
            "WHERE EXISTS (SELECT 1 FROM t b WHERE b.n = a.n)) CYCLE n SET c USING p SELECT * FROM t",
            "Unsupported recursive CTE: self-references inside subqueries are not supported");
    }

    /** Generated columns must not leak into the original recursive term's scope. */
    @ParameterizedTest
    @ValueSource(strings = {"c", "p"})
    public void testGeneratedColumnsNotVisible(String column) throws Exception {
        assertInvalid("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t WHERE " +
            column + " IS NOT NULL) CYCLE n SET c USING p SELECT * FROM t",
            "Column '" + column.toUpperCase(java.util.Locale.ROOT) + "' not found");
    }

    /** Verifies failure during validation, without running a potentially non-terminating query. */
    private void assertInvalid(String sql, String error) throws Exception {
        try (IgnitePlanner planner = plannerCtx(sql, new IgniteSchema(DEFAULT_SCHEMA)).planner()) {
            SqlNode node = planner.parse(sql);

            GridTestUtils.assertThrows(log, () -> planner.validate(node), ValidationException.class, error);
        }
    }
}
