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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.math.BigDecimal;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Ignite convertlet table.
 */
public class IgniteConvertletTable extends ReflectiveConvertletTable {
    /** Instance. */
    public static final IgniteConvertletTable INSTANCE = new IgniteConvertletTable();

    /** */
    private IgniteConvertletTable() {
        // Replace Calcite's convertlet with our own.
        registerOp(SqlStdOperatorTable.TIMESTAMP_DIFF, new TimestampDiffConvertlet());
        registerOp(SqlStdOperatorTable.CAST, this::convertCast);

        addAlias(IgniteOwnSqlOperatorTable.LENGTH, SqlStdOperatorTable.CHAR_LENGTH);
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public SqlRexConvertlet get(SqlCall call) {
        SqlRexConvertlet res = super.get(call);

        return res == null ? StandardConvertletTable.INSTANCE.get(call) : res;
    }

    /** */
    protected RexNode convertCast(
        SqlRexContext cx,
        final SqlCall call
    ) {
        final SqlValidator validator = cx.getValidator();
        final SqlKind kind = call.getKind();
        checkArgument(kind == SqlKind.CAST || kind == SqlKind.SAFE_CAST, kind);
        final boolean safe = kind == SqlKind.SAFE_CAST;
        final SqlNode left = call.operand(0);
        final SqlNode right = call.operand(1);
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final RexNode arg = cx.convertExpression(left);

        if (right instanceof SqlIntervalQualifier) {
            final SqlIntervalQualifier intervalQualifier = (SqlIntervalQualifier)right;
            if (left instanceof SqlIntervalLiteral) {
                RexLiteral srcInterval = (RexLiteral)cx.convertExpression(left);
                BigDecimal srcVal = (BigDecimal)srcInterval.getValue();
                RexLiteral castedInterval = rexBuilder.makeIntervalLiteral(srcVal, intervalQualifier);
                return StandardConvertletTable.castToValidatedType(call, castedInterval, validator, rexBuilder, safe);
            }

            if (arg instanceof RexLiteral && SqlTypeUtil.isNumeric(arg.getType())) {
                BigDecimal srcVal = requireNonNull(((RexLiteral)arg).getValueAs(BigDecimal.class), "sourceValue");
                final BigDecimal multiplier = intervalQualifier.getUnit().multiplier;
                RexLiteral castedInterval =
                    rexBuilder.makeIntervalLiteral(
                        SqlFunctions.multiply(srcVal, multiplier),
                        intervalQualifier);
                return StandardConvertletTable.castToValidatedType(call, castedInterval, validator, rexBuilder, safe);
            }
            return StandardConvertletTable.castToValidatedType(call, arg, validator, rexBuilder, safe);
        }

        return StandardConvertletTable.INSTANCE.convertCall(cx, call);
    }

    /** Convertlet that handles the {@code TIMESTAMPDIFF} function. */
    private static class TimestampDiffConvertlet implements SqlRexConvertlet {
        /** {@inheritDoc} */
        @Override public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // TIMESTAMPDIFF(unit, t1, t2)
            //    => (t2 - t1) UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlIntervalQualifier unitLiteral = call.operand(0);
            TimeUnit unit = unitLiteral.getUnit();
            BigDecimal multiplier = BigDecimal.ONE;
            BigDecimal divider = BigDecimal.ONE;
            SqlTypeName sqlTypeName = unit == TimeUnit.NANOSECOND
                ? SqlTypeName.BIGINT
                : SqlTypeName.INTEGER;
            switch (unit) {
                case MICROSECOND:
                case MILLISECOND:
                case NANOSECOND:
                    divider = unit.multiplier;
                    unit = TimeUnit.MILLISECOND;
                    break;
                case WEEK:
                    multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
                    divider = unit.multiplier;
                    unit = TimeUnit.SECOND;
                    break;
                case QUARTER:
                    divider = unit.multiplier;
                    unit = TimeUnit.MONTH;
                    break;
                default:
                    break;
            }
            final SqlIntervalQualifier qualifier =
                new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
            final RexNode op2 = cx.convertExpression(call.operand(2));
            final RexNode op1 = cx.convertExpression(call.operand(1));
            final RelDataType intervalType =
                cx.getTypeFactory().createTypeWithNullability(
                    cx.getTypeFactory().createSqlIntervalType(qualifier),
                    op1.getType().isNullable() || op2.getType().isNullable());
            final RexCall rexCall = (RexCall)rexBuilder.makeCall(
                intervalType, SqlStdOperatorTable.MINUS_DATE,
                ImmutableList.of(op2, op1));
            final RelDataType intType =
                cx.getTypeFactory().createTypeWithNullability(
                    cx.getTypeFactory().createSqlType(sqlTypeName),
                    SqlTypeUtil.containsNullable(rexCall.getType()));

            RexNode e;

            // Since Calcite converts internal time representation to seconds during cast we need our own cast
            // method to keep fraction of seconds.
            if (unit == TimeUnit.MILLISECOND)
                e = makeCastMilliseconds(rexBuilder, intType, rexCall);
            else
                e = rexBuilder.makeCast(intType, rexCall);

            return rexBuilder.multiplyDivide(e, multiplier, divider);
        }

        /**
         * Creates a call to cast milliseconds interval.
         */
        static RexNode makeCastMilliseconds(RexBuilder builder, RelDataType type, RexNode exp) {
            return builder.ensureType(type, builder.decodeIntervalOrDecimal(exp), false);
        }
    }
}
