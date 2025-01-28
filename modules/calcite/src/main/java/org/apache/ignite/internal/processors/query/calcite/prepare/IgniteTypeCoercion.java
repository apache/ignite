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

import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
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
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteCustomType;
import org.apache.ignite.internal.processors.query.calcite.type.OtherType;
import org.apache.ignite.internal.processors.query.calcite.type.UuidType;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;

/**
 * Implementation of implicit type cast.
 */
public class IgniteTypeCoercion extends TypeCoercionImpl {
    /** Ctor. */
    public IgniteTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
    }

    /** {@inheritDoc} **/
    @Override public boolean binaryComparisonCoercion(SqlCallBinding binding) {
        // Although it is not reflected in the docs, this method is also invoked for MAX, MIN (and other similar operators)
        // by ComparableOperandTypeChecker. When that is the case, fallback to default rules.
        SqlCall call = binding.getCall();

        if (binding.getOperandCount() != 2 || !SqlKind.BINARY_COMPARISON.contains(call.getKind()))
            return super.binaryComparisonCoercion(binding);

        SqlValidatorScope scope = binding.getScope();

        RelDataType leftType = validator.deriveType(scope, call.operand(0));
        RelDataType rightType = validator.deriveType(scope, call.operand(1));

        // Do not create additional CAST node for NULL'ed right value. Any type may get NULL value if is nullable.
        if (leftType.equals(rightType) || (rightType.getFamily() == SqlTypeFamily.NULL && leftType.isNullable()))
            return super.binaryComparisonCoercion(binding);
        else {
            // Find the least restrictive type among the operand types
            // and coerce the operands to that type if such type exists.
            //
            // An example of a least restrictive type from the javadoc for RelDataTypeFactory::leastRestrictive:
            // leastRestrictive(INT, NUMERIC(3, 2)) could be NUMERIC(12, 2)
            //
            // A least restrictive type between types of different type families does not exist -
            // the method returns null (See SqlTypeFactoryImpl::leastRestrictive).
            //
            RelDataType targetType = factory.leastRestrictive(Arrays.asList(leftType, rightType));

            if (targetType == null || targetType.getFamily() == SqlTypeFamily.ANY)
                return super.binaryComparisonCoercion(binding);
            else
                return coerceOperandsType(scope, call, targetType);
        }
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

        // The next block is fully copied from parent class with cutted operand check to SqlDynamicParam.

        // Transform the JavaType to SQL type because the SqlDataTypeSpec
        // does not support deriving JavaType yet.
        if (RelDataTypeFactoryImpl.isJavaType(targetType))
            targetType = ((JavaTypeFactory)factory).toSql(targetType);

        SqlNode operand = call.getOperandList().get(idx);

        requireNonNull(scope, "scope");
        // Check it early.
        if (!needToCast(scope, operand, targetType))
            return false;

        // Fix up nullable attr.
        RelDataType targetType1 = syncAttributes(validator.deriveType(scope, operand), targetType);
        SqlNode desired = castTo(operand, targetType1);
        call.setOperand(idx, desired);
        updateInferredType(desired, targetType1);

        return true;
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


    // The method is fully copied from parent class with cutted operand check to SqlDynamicParam, which not supported.

    /** {@inheritDoc} */
    @Override protected boolean coerceColumnType(
        @Nullable SqlValidatorScope scope,
        SqlNodeList nodeList,
        int idx,
        RelDataType targetType
    ) {
        // Transform the JavaType to SQL type because the SqlDataTypeSpec
        // does not support deriving JavaType yet.
        if (RelDataTypeFactoryImpl.isJavaType(targetType))
            targetType = ((JavaTypeFactory)factory).toSql(targetType);

        // This will happen when there is a star/dynamic-star column in the select list,
        // and the source is values expression, i.e. `select * from (values(1, 2, 3))`.
        // There is no need to coerce the column type, only remark
        // the inferred row type has changed, we will then add in type coercion
        // when expanding star/dynamic-star.

        // See SqlToRelConverter#convertSelectList for details.
        if (idx >= nodeList.size()) {
            // Can only happen when there is a star(*) in the column,
            // just return true.
            return true;
        }

        final SqlNode node = nodeList.get(idx);
        if (node instanceof SqlIdentifier) {
            // Do not expand a star/dynamic table col.
            SqlIdentifier node1 = (SqlIdentifier)node;
            if (node1.isStar())
                return true;
            else if (DynamicRecordType.isDynamicStarColName(Util.last(node1.names))) {
                // Should support implicit cast for dynamic table.
                return false;
            }
        }

        requireNonNull(scope, "scope is needed for needToCast(scope, operand, targetType)");
        if (node instanceof SqlCall) {
            SqlCall node2 = (SqlCall)node;
            if (node2.getOperator().kind == SqlKind.AS) {
                final SqlNode operand = node2.operand(0);
                if (!needToCast(scope, operand, targetType))
                    return false;

                RelDataType targetType2 = syncAttributes(validator.deriveType(scope, operand), targetType);
                final SqlNode casted = castTo(operand, targetType2);
                node2.setOperand(0, casted);
                updateInferredType(casted, targetType2);
                return true;
            }
        }

        if (!needToCast(scope, node, targetType))
            return false;

        RelDataType targetType3 = syncAttributes(validator.deriveType(scope, node), targetType);
        final SqlNode node3 = castTo(node, targetType3);
        nodeList.set(idx, node3);
        updateInferredType(node3, targetType3);
        return true;
    }

    /**
     * Sync the data type additional attributes before casting, i.e. nullability, charset, collation.
     */
    private RelDataType syncAttributes(
        RelDataType fromType,
        RelDataType toType
    ) {
        RelDataType syncedType = toType;
        if (fromType != null) {
            syncedType = factory.createTypeWithNullability(syncedType, fromType.isNullable());
            if (SqlTypeUtil.inCharOrBinaryFamilies(fromType)
                && SqlTypeUtil.inCharOrBinaryFamilies(toType)) {
                Charset charset = fromType.getCharset();
                if (charset != null && SqlTypeUtil.inCharFamily(syncedType)) {
                    SqlCollation collation = getCollation(fromType);
                    syncedType = factory.createTypeWithCharsetAndCollation(syncedType,
                        charset,
                        collation);
                }
            }
        }
        return syncedType;
    }

    /** */
    private static SqlNode castTo(SqlNode node, RelDataType type) {
        return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node,
            SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
    }
}
