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
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.calcite.PseudoColumnDescriptor;
import org.apache.ignite.calcite.PseudoColumnProvider;
import org.apache.ignite.calcite.PseudoColumnValueExtractorContext;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexImpTable;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class RowIdPseudoColumnTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg)
            .setPluginProviders(new RowIdPseudoColumnPluginProvider());
    }

    @Test
    public void name() {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        for (int i = 0; i < 2; i++)
            sql("insert into PUBLIC.PERSON(id, name) values(?, ?)", i, "foo" + i);

        // TODO: IGNITE-28223-add-rowid Вот тут теперь падает и можно дальше двигать
        assertQuery("select id, name, rowid from PUBLIC.PERSON where rowid = '0'")
            .columnNames("ID", "NAME", "ROWID")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", "_key_PK"))
            .returns(0, "foo0", "0")
            .check();
    }

    /** */
    public static @Nullable Integer toKeyFromRowId(@Nullable String rowId) {
        return rowId == null ? null : Integer.parseInt(rowId);
    }

    /** */
    private static class RowIdPseudoColumnPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (PseudoColumnProvider.class.equals(cls))
                return (T)(PseudoColumnProvider)() -> List.of(new RowIdPseudoColumn());
            else if (FrameworkConfig.class.equals(cls)) {
                return (T)Frameworks.newConfigBuilder(CalciteQueryProcessor.FRAMEWORK_CONFIG)
                    .operatorTable(SqlOperatorTables.chain(
                        new RowIdPseudoColumnOperatorTable().init(),
                        CalciteQueryProcessor.FRAMEWORK_CONFIG.getOperatorTable()
                    ))
                    .build();
            }

            return super.createComponent(ctx, cls);
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
            RexImpTable.INSTANCE.define(
                RowIdPseudoColumnOperatorTable.TO_KEY_FROM_ROW_ID,
                RexImpTable.createRexCallImplementor((translator, call, translatedOperands) -> {
                    var str = Expressions.convert_(translatedOperands.get(0), String.class);

                    return Expressions.call(RowIdPseudoColumnTest.class, "toKeyFromRowId", str);
                }, NullPolicy.ANY, false)
            );
        }
    }

    /** */
    public static class RowIdPseudoColumnOperatorTable extends ReflectiveSqlOperatorTable {
        /** */
        public static final SqlFunction TO_KEY_FROM_ROW_ID = new SqlFunction(
            "TO_KEY_FROM_ROW_ID",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER,
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    /** */
    private static class RowIdPseudoColumn implements PseudoColumnDescriptor {
        /** {@inheritDoc} */
        @Override public String name() {
            return "ROWID";
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return String.class;
        }

        /** {@inheritDoc} */
        @Override public int scale() {
            return PseudoColumnDescriptor.NOT_SPECIFIED;
        }

        /** {@inheritDoc} */
        @Override public int precision() {
            return PseudoColumnDescriptor.NOT_SPECIFIED;
        }

        /** {@inheritDoc} */
        @Override public Object value(PseudoColumnValueExtractorContext ctx) throws IgniteCheckedException {
            return ctx.source(true, true).toString();
        }
    }
}
