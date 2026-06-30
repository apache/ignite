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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest.createTable;

/**
 * Constructs SQL statements checks.
 *
 * <pre>
 *     // Checks that plan validation throws no errors.
 *     new StatementChecker().sql("SELECT 1 + 1").ok();
 *
 *     // Checks that plan validation fails and a error message contains the given string.
 *     new StatementChecker().sql("SELECT").fails("Parse error");
 *
 * </pre>
 *
 * <p>Default test name consists of expected result {@code ok()} if test should pass and {@code fail()} if it should fail,
 * an SQL statement string, and dynamic parameters.
 * <pre>
 *     new StatementChecker().sql("SELECT 1").ok(); // OK SELECT 1
 *     new StatementChecker().sql("SELECT t").fails(); // ERR SELECT t
 *     new StatementChecker().sql("SELECT ?", 1).ok(); // OK SELECT 1, params=1
 * </pre>
 *
 * <p><b>Schema initialization</b>
 *
 * <pre>
 *     new StatementChecker()
 *         .table("t1", "int_col", Integer.class)
 *         .sql("SELECT int_col FROM t1")
 *         .ok();
 * </pre>
 */
public class StatementChecker {
    /** */
    private String sqlStatement;

    /** */
    private List<Object> dynamicParams;

    /** */
    private final SqlPrepare sqlPrepare;

    /** */
    final Map<String, TestTable> testTables = new HashMap<>();

    /** */
    private String[] rulesToDisable = new String[0];

    /** */
    private Consumer<StatementChecker> setup = (checker) -> {
    };

    /** */
    public StatementChecker(SqlPrepare sqlPrepare) {
        this.sqlPrepare = sqlPrepare;
    }

    /**
     * Prepares the given SQL statement string.
     */
    @FunctionalInterface
    public interface SqlPrepare {
        /**
         * Validates and converts the given SQL statement into a physical plan.
         *
         * @param schema         A schema.
         * @param sql            An SQL statement.
         * @param params         A list of dynamic parameters.
         * @param rulesToDisable A list of rules to exclude from optimisation.
         */
        T2<IgniteRel, IgnitePlanner> prepare(
            IgniteSchema schema, String sql, List<Object> params, String... rulesToDisable
        ) throws Exception;
    }

    /** Initial schema initialisation. */
    protected IgniteSchema initSchema() {
        setup.accept(this);
        IgniteSchema schema = new IgniteSchema("PUBLIC");

        for (TestTable tbl : testTables.values())
            schema.addTable(tbl.name(), tbl);

        return schema;
    }

    /**
     * Sets a function that is going to be called prior to test run.
     */
    public StatementChecker setup(Consumer<StatementChecker> setup) {
        this.setup = setup;
        return this;
    }

    /**
     * Sets rules to exclude from optimisation.
     */
    public StatementChecker disableRules(String... rulesToDisable) {
        this.rulesToDisable = rulesToDisable;

        return this;
    }

    /**
     * Updates schema to include a table with 1 column.
     */
    public StatementChecker table(String tableName, String colName, Class<?> colType) {
        testTables.put(tableName, createTable(tableName, IgniteDistributions.single(), colName, colType));

        return this;
    }

    /** Executes default checks. Runs before a check provide by {@link StatementChecker#ok()}. */
    protected void checkRel(IgniteRel igniteRel, IgnitePlanner planner, IgniteSchema schema) {
    }

    /** Sets an SQL statement string and dynamic parameters. */
    public StatementChecker sql(String template, Object... params) {
        return sql(() -> template, params);
    }

    /**
     * Sets an SQL statement string and dynamic parameters.
     * Existing to make writing multiline SQL statements easier.
     */
    public StatementChecker sql(Supplier<String> template, Object... params) {
        sqlStatement = template.get();
        dynamicParams = Arrays.asList(params);
        return this;
    }

    /** Expect that validation succeeds. */
    public void ok() throws Exception {
        IgniteSchema schema = initSchema();
        T2<IgniteRel, IgnitePlanner> res = sqlPrepare.prepare(schema, sqlStatement, dynamicParams, rulesToDisable);

        try (IgnitePlanner planner = res.get2()) {
            checkRel(res.get1(), planner, schema);
        }
    }

    /** Validation is expected to fail with an error that contains the given message. */
    public void fails(String errorMessage) {
        IgniteSchema schema = initSchema();
        boolean expect = false;

        try {
            T2<IgniteRel, IgnitePlanner> res = sqlPrepare.prepare(schema, sqlStatement, dynamicParams, rulesToDisable);

            try (IgnitePlanner planner = res.get2()) {
                checkRel(res.get1(), planner, schema);
            }
        }
        catch (Throwable t) {
            String msg = t.getMessage();
            if (msg == null || !msg.contains(errorMessage))
                throw new AssertionError("Unexpected message: " + msg, t);
            else
                expect = true;
        }

        if (!expect)
            throw new AssertionError("Expected statement check not failed");
    }
}
