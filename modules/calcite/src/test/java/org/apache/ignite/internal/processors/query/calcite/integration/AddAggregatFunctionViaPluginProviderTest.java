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

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Optionality;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorFactoryProvider;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulators.AbstractAccumulator;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.jspecify.annotations.Nullable;
import org.junit.Test;

/** Test for adding aggregat function via {@link PluginProvider}. */
public class AddAggregatFunctionViaPluginProviderTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String TEST_SUM_FUN_NAME = "TEST_SUM";

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new TestPluginProvider());
    }

    /** */
    @Test
    public void test() {
        assertQuery("SELECT TEST_SUM(x) FROM (VALUES (1), (2), (3)) t(x)")
            .returns(6L)
            .check();
    }

    /** */
    private static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getName();
        }

        /** {@inheritDoc} */
        @Override public @Nullable <T> T createComponent(PluginContext ctx, Class<T> cls) {
            if (!FrameworkConfig.class.equals(cls))
                return null;

            FrameworkConfig cfg = CalciteQueryProcessor.FRAMEWORK_CONFIG;

            return (T)Frameworks.newConfigBuilder(cfg)
                .operatorTable(SqlOperatorTables.chain(
                    new TestSqlOperatorTable().init(), cfg.getOperatorTable()
                ))
                .context(Contexts.chain(cfg.getContext(), Contexts.of(new TestSumAccumulatorFactoryProvider())))
                .build();
        }
    }

    /** */
    public static class TestSqlSumAggFunction extends SqlAggFunction {
        /** */
        public TestSqlSumAggFunction() {
            super(
                TEST_SUM_FUN_NAME,
                null,
                SqlKind.SUM,
                ReturnTypes.AGG_SUM,
                null,
                OperandTypes.NUMERIC,
                SqlFunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN
            );
        }
    }

    /** */
    public static class TestSqlOperatorTable extends ReflectiveSqlOperatorTable {
        /** */
        @SuppressWarnings("unused")
        public static final SqlAggFunction TEST_SUM = new TestSqlSumAggFunction();
    }

    /** */
    private static class TestSum<Row> extends AbstractAccumulator<Row> {
        /** */
        private long sum;

        /** */
        protected TestSum(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Number val = get(0, row);

            if (val != null)
                sum += val.longValue();
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            sum += ((TestSum<Row>)other).sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
        }
    }

    /** */
    private static class TestSumAccumulatorFactoryProvider implements AccumulatorFactoryProvider {
        /** {@inheritDoc} */
        @Override public @Nullable <Row> Supplier<Accumulator<Row>> factory(AggregateCall call, ExecutionContext<Row> ctx) {
            if (call.getAggregation().getName().equals(TEST_SUM_FUN_NAME))
                return () -> new TestSum<>(call, ctx.rowHandler());

            return null;
        }
    }
}
