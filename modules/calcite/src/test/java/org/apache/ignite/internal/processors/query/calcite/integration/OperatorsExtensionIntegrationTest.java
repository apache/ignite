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

import java.math.BigDecimal;
import java.sql.Timestamp;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexImpTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteConvertletTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteSqlCallRewriteTable;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.junit.Test;

/** */
public class OperatorsExtensionIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final SqlFunction SUBSTR =
        new SqlFunction(
            "SUBSTR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING,
            null,
            OperandTypes.STRING_INTEGER_INTEGER,
            SqlFunctionCategory.STRING);

    /** */
    private static final SqlFunction TRUNC =
        new SqlFunction(
            "TRUNC",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_OR_EXACT_NO_SCALE,
            null,
            OperandTypes.TIMESTAMP,
            SqlFunctionCategory.TIMEDATE);

    /** */
    private static final SqlFunction TO_NUMBER =
        new SqlFunction(
            "TO_NUMBER",
            SqlKind.OTHER_FUNCTION,
            opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.DECIMAL),
            null,
            OperandTypes.STRING,
            SqlFunctionCategory.NUMERIC);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new AbstractTestPluginProvider() {
                @Override public String name() {
                    return "Test operators extension plugin";
                }

                @Override public void start(PluginContext ctx) {
                    IgniteOwnSqlOperatorTable.instance().register(SUBSTR);
                    IgniteOwnSqlOperatorTable.instance().register(TRUNC);
                    IgniteOwnSqlOperatorTable.instance().register(TO_NUMBER);

                    // Tests operator extension via implementor.
                    try {
                        RexImpTable.INSTANCE.defineMethod(
                            TO_NUMBER,
                            OperatorsExtensionIntegrationTest.class.getMethod("toNumber", String.class),
                            NullPolicy.STRICT
                        );
                    }
                    catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    }
                    // Tests operator extension as an alias.
                    IgniteConvertletTable.INSTANCE.addAlias(SUBSTR, SqlStdOperatorTable.SUBSTRING);
                    // Tests operator extension via covnertlet.
                    IgniteConvertletTable.INSTANCE.registerOp(TRUNC, new TruncConvertlet());
                    // Tests operator extension via SQL rewrite.
                    IgniteSqlCallRewriteTable.INSTANCE.register("LTRIM", OperatorsExtensionIntegrationTest::rewriteLtrim);
                }
            });
    }

    /**
     * Tests ...
     */
    @Test
    public void test() throws Exception {
        assertQuery("SELECT substr('12345', 3, 2)").returns("34").check();
        assertQuery("SELECT to_number('12.34')").returns(new BigDecimal("12.34")).check();
        assertQuery("SELECT ltrim('aabcda', 'a')").returns("bcda").check();
        assertQuery("SELECT trunc(TIMESTAMP '2021-01-01 01:02:03')")
            .returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
    }

    /**
     *
     */
    public static SqlCall rewriteLtrim(SqlValidator validator, SqlCall call) {
        if (call.operandCount() != 2)
            return call;

        return SqlStdOperatorTable.TRIM.createCall(
            call.getParserPosition(),
            SqlTrimFunction.Flag.LEADING.symbol(SqlParserPos.ZERO),
            call.operand(1),
            call.operand(0)
        );
    }

    /**
     * Implementor for {@code TO_NUMBER} function.
     */
    public static BigDecimal toNumber(String s) {
        return new BigDecimal(s);
    }

    /**
     * Convertlet that handles the {@code TRUNC} function and convert it to FLOOR function.
     */
    private static class TruncConvertlet implements SqlRexConvertlet {
        /** {@inheritDoc} */
        @Override public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final RexBuilder rexBuilder = cx.getRexBuilder();
            RexNode day = rexBuilder.makeLiteral(TimeUnitRange.DAY, cx.getTypeFactory().createSqlType(SqlTypeName.SYMBOL));
            return rexBuilder.makeCall(SqlStdOperatorTable.FLOOR,
                ImmutableList.of(cx.convertExpression(call.operand(0)), day));
        }
    }
}
