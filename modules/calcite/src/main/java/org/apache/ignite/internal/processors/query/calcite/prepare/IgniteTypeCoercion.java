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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteCustomType;
import org.apache.ignite.internal.processors.query.calcite.type.OtherType;
import org.apache.ignite.internal.processors.query.calcite.type.UuidType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of implicit type cast.
 */
public class IgniteTypeCoercion extends TypeCoercionImpl {
    /** Ctor. */
    public IgniteTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
    }

    /** {@inheritDoc} */
    @Override public @Nullable RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
        RelDataType res = super.implicitCast(in, expected);

        if (res == null) {
            // FLOAT/DOUBLE -> LONG/INTEGER/SHORT/BYTE
            if (SqlTypeUtil.isApproximateNumeric(in) && expected == SqlTypeFamily.INTEGER)
                return expected.getDefaultConcreteType(factory);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected boolean coerceOperandType(
        SqlValidatorScope scope,
        SqlCall call,
        int idx,
        RelDataType targetType
    ) {
        if (targetType instanceof IgniteCustomType) {
            SqlNode operand = call.getOperandList().get(idx);

            if (operand instanceof SqlDynamicParam)
                return false;

            RelDataType fromType = validator.deriveType(scope, operand);

            if (fromType == null)
                return false;

            if (SqlTypeUtil.inCharFamily(fromType) || targetType instanceof OtherType) {
                targetType = factory.createTypeWithNullability(targetType, fromType.isNullable());

                SqlNode desired = SqlStdOperatorTable.CAST.createCall(
                    SqlParserPos.ZERO,
                    operand,
                    new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec(targetType.toString(), SqlParserPos.ZERO),
                        SqlParserPos.ZERO).withNullable(targetType.isNullable())
                );

                call.setOperand(idx, desired);
                updateInferredType(desired, targetType);

                return true;
            }
            else
                return false;
        }
        else if (SqlTypeUtil.isIntType(targetType)) {
            SqlNode operand = call.getOperandList().get(idx);

            RelDataType fromType = validator.deriveType(scope, operand);

            if (SqlTypeUtil.isApproximateNumeric(fromType)) {
                SqlNode desired = SqlStdOperatorTable.CAST.createCall(
                    SqlParserPos.ZERO,
                    operand,
                    new SqlDataTypeSpec(SqlTypeUtil.convertTypeToSpec(targetType).getTypeNameSpec(),
                        SqlParserPos.ZERO).withNullable(targetType.isNullable())
                );

                call.setOperand(idx, desired);
                updateInferredType(desired, targetType);

                return true;
            }
        }

        return super.coerceOperandType(scope, call, idx, targetType);
    }

    /** {@inheritDoc} */
    @Override public RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2) {
        if (type1 == null || type2 == null)
            return null;

        if (type1 instanceof UuidType && SqlTypeUtil.isCharacter(type2))
            return type1;

        if (type2 instanceof UuidType && SqlTypeUtil.isCharacter(type1))
            return type2;

        return super.commonTypeForBinaryComparison(type1, type2);
    }

    /** {@inheritDoc} */
    @Override protected boolean needToCast(SqlValidatorScope scope, SqlNode node, RelDataType toType) {
        if (SqlTypeUtil.isInterval(toType)) {
            RelDataType fromType = validator.deriveType(scope, node);

            if (SqlTypeUtil.isInterval(fromType)) {
                // Two different families of intervals: INTERVAL_DAY_TIME and INTERVAL_YEAR_MONTH.
                return fromType.getSqlTypeName().getFamily() != toType.getSqlTypeName().getFamily();
            }
        }
        else if (SqlTypeUtil.isIntType(toType)) {
            RelDataType fromType = validator.deriveType(scope, node);

            if (fromType == null)
                return false;

            if (SqlTypeUtil.isIntType(fromType) && fromType.getSqlTypeName() != toType.getSqlTypeName())
                return true;
        }

        return super.needToCast(scope, node, toType);
    }
}
