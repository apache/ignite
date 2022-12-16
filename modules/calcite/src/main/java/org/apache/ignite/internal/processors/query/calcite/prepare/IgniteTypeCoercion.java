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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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
        return super.implicitCast(in, expected);
    }

    @Override public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
        return super.binaryArithmeticCoercion(binding);
    }

    @Override
    protected boolean binaryArithmeticWithStrings(SqlCallBinding binding, RelDataType left, RelDataType right) {
        return super.binaryArithmeticWithStrings(binding, left, right);
    }

    @Override public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        return super.binaryComparisonCoercion(binding);
    }

    @Override protected @Nullable RelDataType commonTypeForComparison(List<RelDataType> dataTypes) {
        return super.commonTypeForComparison(dataTypes);
    }

    @Override public boolean caseWhenCoercion(SqlCallBinding callBinding) {
        return super.caseWhenCoercion(callBinding);
    }

    @Override public boolean userDefinedFunctionCoercion(SqlValidatorScope scope, SqlCall call, SqlFunction function) {
        return super.userDefinedFunctionCoercion(scope, call, function);
    }

    @Override public boolean querySourceCoercion(@Nullable SqlValidatorScope scope, RelDataType sourceRowType,
        RelDataType targetRowType, SqlNode query) {
        return super.querySourceCoercion(scope, sourceRowType, targetRowType, query);
    }

    @Override
    protected boolean coerceOperandsType(@Nullable SqlValidatorScope scope, SqlCall call, RelDataType commonType) {
        return super.coerceOperandsType(scope, call, commonType);
    }

    @Override protected boolean coerceColumnType(@Nullable SqlValidatorScope scope, SqlNodeList nodeList, int index,
        RelDataType targetType) {
        return super.coerceColumnType(scope, nodeList, index, targetType);
    }

    @Override protected void updateInferredType(SqlNode node, RelDataType type) {
        super.updateInferredType(node, type);
    }

    @Override protected void updateInferredColumnType(SqlValidatorScope scope, SqlNode query, int columnIndex,
        RelDataType desiredType) {
        super.updateInferredColumnType(scope, query, columnIndex, desiredType);
    }

    @Override
    public @Nullable RelDataType getTightestCommonType(@Nullable RelDataType type1, @Nullable RelDataType type2) {
        return super.getTightestCommonType(type1, type2);
    }

    /** {@inheritDoc} */
    @Override public boolean inOperationCoercion(SqlCallBinding binding) {
        return super.inOperationCoercion(binding);
    }

    /** {@inheritDoc} */
    @Override public boolean builtinFunctionCoercion(SqlCallBinding binding, List<RelDataType> operandTypes,
        List<SqlTypeFamily> expectedFamilies) {
        if(!super.builtinFunctionCoercion(binding, operandTypes, expectedFamilies))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean coerceOperandType(
        SqlValidatorScope scope,
        SqlCall call1,
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

    @Override public boolean rowTypeCoercion(@Nullable SqlValidatorScope scope, SqlNode query, int columnIndex,
        RelDataType targetType) {
        return super.rowTypeCoercion(scope, query, columnIndex, targetType);
    }
}
