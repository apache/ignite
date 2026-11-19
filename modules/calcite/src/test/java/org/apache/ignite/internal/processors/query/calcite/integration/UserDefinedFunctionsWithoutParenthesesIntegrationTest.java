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

import java.util.Arrays;
import java.util.Collections;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlTableFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junit.SystemPropertiesExtension;
import org.apache.ignite.testframework.junit.WithSystemProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR;

/** Tests calls to user-defined functions without parentheses. */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR, value = "true")
public class UserDefinedFunctionsWithoutParenthesesIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CalciteQueryEngineConfiguration calciteCfg = new CalciteQueryEngineConfiguration()
            .setAllowUdfCallsWithoutParentheses(true);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(calciteCfg);

        return cfg;
    }

    /** */
    @BeforeEach
    public void registerFunctions() {
        client.getOrCreateCache(new CacheConfiguration<Integer, Object>("parameterless-udfs")
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(ParameterlessFunctions.class));
    }

    /** */
    @Test
    public void testScalarFunctions() {
        assertSame(FRAMEWORK_CONFIG, queryProcessor(client).frameworkConfig());

        assertQuery("SELECT my_udf, my_udf(), PUBLIC.my_udf, my_udf(7)").returns(42, 42, 42, 7).check();
        assertQuery("SELECT my_udf + x FROM (VALUES (1), (2)) t(x) WHERE x < my_udf ORDER BY x")
            .ordered().returns(43).returns(44).check();
        assertQuery("VALUES (my_udf)").returns(42).check();
        assertQuery("SELECT * FROM (VALUES (my_udf))").returns(42).check();
        assertQuery("SELECT x FROM (VALUES (2), (1)) t(x) ORDER BY my_udf, x")
            .ordered().returns(1).returns(2).check();
    }

    /** */
    @Test
    public void testTableFunctionArguments() {
        assertQuery("SELECT * FROM TABLE(my_table_udf())").returns(1).returns(2).check();
        assertQuery("SELECT * FROM TABLE(table_with_argument(my_udf))").returns(42).check();
    }

    /** Tests names of rewritten projections alongside stars and other expressions. */
    @Test
    public void testColumnNames() {
        assertQuery("SELECT my_udf, my_udf()")
            .columnNames("MY_UDF", "MY_UDF()").returns(42, 42).check();
        assertQuery("SELECT *, my_udf, x + 1 AS shifted FROM (VALUES (1)) t(x)")
            .columnNames("X", "MY_UDF", "SHIFTED").returns(1, 42, 2).check();
        assertQuery("SELECT *, my_udf, x + 1, my_udf() FROM (VALUES (1)) t(x)")
            .columnNames("X", "MY_UDF", "X + 1", "MY_UDF()").returns(1, 42, 2, 42).check();
        assertQuery("SELECT t.*, my_udf, x AS shifted FROM (VALUES (1)) t(x)")
            .columnNames("X", "MY_UDF", "SHIFTED").returns(1, 42, 1).check();
    }

    /** Tests UDF calls and column precedence within UNNEST operands. */
    @Test
    public void testUnnest() {
        assertQuery("SELECT * FROM UNNEST(ARRAY[my_udf])").returns(42).check();
        assertQuery("SELECT * FROM UNNEST(ARRAY[my_udf()])").returns(42).check();
        assertQuery("SELECT * FROM UNNEST(ARRAY[PUBLIC.my_udf]) AS u(my_udf)").returns(42).check();
        assertQuery("SELECT v FROM (VALUES (7)) t(my_udf), UNNEST(ARRAY[my_udf]) AS u(v)").returns(7).check();
        assertQuery("SELECT v FROM (VALUES (7)) t(my_udf), UNNEST(ARRAY[t.my_udf]) AS u(v)").returns(7).check();
        assertQuery("SELECT v FROM (VALUES (7)) t(x), UNNEST(ARRAY[my_udf + x]) AS u(v)").returns(49).check();
    }

    /** Tests that scalar subqueries rewrite UDF calls before inferring operand types. */
    @Test
    public void testScalarSubqueries() {
        assertQuery("SELECT (SELECT my_udf + 1), (SELECT 1 + my_udf), (SELECT my_udf() + 1)")
            .returns(43, 43, 43).check();
        assertQuery("SELECT (SELECT (SELECT my_udf + 1))").returns(43).check();
        assertQuery("VALUES ((SELECT my_udf + 1))").returns(43).check();
        assertQuery("SELECT (SELECT my_udf + ?)").withParams(1).returns(43).check();
        assertQuery("SELECT (SELECT CASE WHEN my_udf > 0 THEN my_udf + 1 ELSE 0 END)").returns(43).check();
        assertQuery("SELECT * FROM TABLE(table_with_argument((SELECT my_udf + 1)))").returns(43).check();
        assertQuery("SELECT (SELECT v FROM UNNEST(ARRAY[my_udf + 1]) AS u(v))").returns(43).check();
    }

    /** Tests column visibility and precedence in correlated scalar subqueries. */
    @Test
    public void testCorrelatedScalarSubqueries() {
        assertQuery("SELECT x, (SELECT my_udf + x) FROM (VALUES (1), (2)) t(x) ORDER BY x")
            .ordered().returns(1, 43).returns(2, 44).check();
        assertQuery("SELECT (SELECT my_udf + 1) FROM (VALUES (7)) t(my_udf)").returns(8).check();
        assertQuery("SELECT (SELECT my_udf + 1 FROM (VALUES (7)) i(my_udf)) FROM (VALUES (2)) t(my_udf)")
            .returns(8).check();
        assertQuery("SELECT x, (SELECT v FROM UNNEST(ARRAY[my_udf + x]) AS u(v)) "
            + "FROM (VALUES (1), (2)) t(x) ORDER BY x").ordered().returns(1, 43).returns(2, 44).check();
    }

    /** */
    @Test
    public void testColumnAndAliasPrecedence() {
        assertQuery("SELECT my_udf, my_udf() FROM (VALUES (7)) t(my_udf)").returns(7, 42).check();
        assertQuery("SELECT PUBLIC.my_udf FROM (VALUES (8)) PUBLIC(my_udf)").returns(8).check();
        assertQuery("SELECT x AS my_udf, COUNT(*) FROM (VALUES (1), (2)) t(x) GROUP BY my_udf ORDER BY my_udf")
            .ordered().returns(1, 1L).returns(2, 1L).check();

        assertThrows("SELECT my_udf FROM (VALUES (1)) a(my_udf), (VALUES (2)) b(my_udf)",
            SqlValidatorException.class, "Column 'MY_UDF' is ambiguous");
    }

    /** */
    @Test
    public void testGroupByAndHaving() {
        assertQuery("SELECT my_udf, COUNT(*) FROM (VALUES (1), (2)) t(x) GROUP BY 1 HAVING my_udf > 0")
            .returns(42, 2L).check();
        assertQuery("SELECT my_udf, COUNT(*) FROM (VALUES (1), (2)) t(x) GROUP BY my_udf HAVING my_udf > 0")
            .returns(42, 2L).check();
    }

    /** */
    @Test
    public void testQualify() {
        assertQuery("SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM (VALUES (1), (43)) t(x) "
            + "QUALIFY x < my_udf AND rn > 0").returns(1, 1L).check();
        assertQuery("SELECT x AS my_udf, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM (VALUES (1), (43)) t(x) "
            + "QUALIFY my_udf < 2 AND rn > 0").returns(1, 1L).check();
    }

    /** */
    @Test
    public void testFetch() {
        assertQuery("SELECT x FROM (VALUES (1), (2)) t(x) ORDER BY x FETCH FIRST (my_udf - 41) ROWS ONLY")
            .returns(1).check();
    }

    /** */
    @Test
    public void testInvalidCalls() {
        assertThrows("SELECT with_argument", SqlValidatorException.class, "Column 'WITH_ARGUMENT' not found");
    }

    /** */
    @Test
    public void testView() {
        sql("CREATE VIEW parameterless_udf_view AS SELECT my_udf FROM TABLE(my_table_udf())");

        try {
            assertQuery("SELECT my_udf FROM parameterless_udf_view").returns(42).returns(42).check();
        }
        finally {
            sql("DROP VIEW parameterless_udf_view");
        }
    }

    /** Tests that CREATE TABLE AS SELECT preserves the query engine configuration. */
    @Test
    public void testCreateTableAsSelect() {
        sql("CREATE TABLE parameterless_udf_ctas AS SELECT x, my_udf AS val FROM TABLE(my_table_udf())");

        try {
            assertQuery("SELECT x, val FROM parameterless_udf_ctas").returns(1, 42).returns(2, 42).check();
        }
        finally {
            sql("DROP TABLE parameterless_udf_ctas");
        }
    }

    /** SQL functions used to test calls without parentheses. */
    public static class ParameterlessFunctions {
        /** */
        @QuerySqlFunction(alias = "my_udf")
        public static int scalar() {
            return 42;
        }

        /** */
        @QuerySqlFunction(alias = "my_udf")
        public static int scalar(int val) {
            return val;
        }

        /** */
        @QuerySqlFunction(alias = "with_argument")
        public static int withArgument(int val) {
            return val;
        }

        /** */
        @QuerySqlTableFunction(alias = "my_table_udf", columnNames = {"X"}, columnTypes = {Integer.class})
        public static Iterable<Object[]> table() {
            return Arrays.asList(new Object[] {1}, new Object[] {2});
        }

        /** */
        @QuerySqlTableFunction(alias = "table_with_argument", columnNames = {"X"}, columnTypes = {Integer.class})
        public static Iterable<Object[]> tableWithArgument(int val) {
            return Collections.singletonList(new Object[] {val});
        }
    }
}
