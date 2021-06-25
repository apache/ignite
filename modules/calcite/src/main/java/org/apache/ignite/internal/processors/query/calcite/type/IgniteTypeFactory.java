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

package org.apache.ignite.internal.processors.query.calcite.type;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Ignite type factory.
 */
public class IgniteTypeFactory extends JavaTypeFactoryImpl {
    /** */
    public IgniteTypeFactory() {
        super(IgniteTypeSystem.INSTANCE);
    }

    /**
     * @param typeSystem Type system.
     */
    public IgniteTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    /** {@inheritDoc} */
    @Override public Type getJavaClass(RelDataType type) {
        if (type instanceof JavaType)
            return ((JavaType)type).getJavaClass();
        else if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
            switch (type.getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    return String.class;
                case DATE:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                case FLOAT:
                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return ByteString.class;
                case GEOMETRY:
                    return Geometries.Geom.class;
                case SYMBOL:
                    return Enum.class;
                case ANY:
                case OTHER:
                    return Object.class;
                case NULL:
                    return Void.class;
                default:
                    break;
            }
        }
        switch (type.getSqlTypeName()) {
            case ROW:
                return Object[].class; // At now
            case MAP:
                return Map.class;
            case ARRAY:
            case MULTISET:
                return List.class;
            default:
                return null;
        }
    }

    /**
     * @param type Field logical type.
     * @return Result type.
     */
    public Type getResultClass(RelDataType type) {
        if (type instanceof JavaType)
            return ((JavaType)type).getJavaClass();
        else if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
            switch (type.getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    return String.class;
                case DATE:
                    return java.sql.Date.class;
                case TIME:
                    return java.sql.Time.class;
                case TIMESTAMP:
                    return Timestamp.class;
                case TIME_WITH_LOCAL_TIME_ZONE:
                    return LocalTime.class;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return LocalDateTime.class;
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return type.isNullable() ? Integer.class : int.class;
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                case FLOAT:
                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return byte[].class;
                case GEOMETRY:
                    return Geometries.Geom.class;
                case SYMBOL:
                    return Enum.class;
                case ANY:
                case OTHER:
                    return Object.class;
                case NULL:
                    return Void.class;
                default:
                    break;
            }
        }
        switch (type.getSqlTypeName()) {
            case ROW:
                return Object[].class; // At now
            case MAP:
                return Map.class;
            case ARRAY:
            case MULTISET:
                return List.class;
            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        if (types.size() == 1 || allEquals(types))
            return F.first(types);

        RelDataType type0 = types.get(0);
        if (type0.getSqlTypeName() != null) {
            RelDataType resultType = leastRestrictiveSqlType(types);
            if (resultType != null)
                return resultType;
            return leastRestrictiveByCast(types);
        }

        return ((RelDataTypeFactoryImpl)this).leastRestrictive(types);
    }

    private RelDataType leastRestrictiveByCast(List<RelDataType> types) {
        RelDataType resultType = types.get(0);
        boolean anyNullable = resultType.isNullable();
        for (int i = 1; i < types.size(); i++) {
            RelDataType type = types.get(i);
            if (type.getSqlTypeName() == SqlTypeName.NULL) {
                anyNullable = true;
                continue;
            }

            if (type.isNullable())
                anyNullable = true;

            if (SqlTypeUtil.canCastFrom(type, resultType, false))
                resultType = type;
            else {
                if (!SqlTypeUtil.canCastFrom(resultType, type, false))
                    return null;
            }
        }
        if (anyNullable)
            return createTypeWithNullability(resultType, true);
        else
            return resultType;
    }

    private RelDataType leastRestrictiveSqlType(List<RelDataType> types) {
        RelDataType resType = null;
        int nullCnt = 0;
        int nullableCnt = 0;
        int javaCnt = 0;
        int anyCnt = 0;

        for (RelDataType type : types) {
            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == null)
                return null;
            if (typeName == SqlTypeName.ANY)
                anyCnt++;
            if (type.isNullable())
                ++nullableCnt;
            if (typeName == SqlTypeName.NULL)
                ++nullCnt;
            if (isJavaType(type))
                ++javaCnt;
        }

        //  if any of the inputs are ANY, the output is ANY
        if (anyCnt > 0) {
            return createTypeWithNullability(createSqlType(SqlTypeName.ANY),
                nullCnt > 0 || nullableCnt > 0);
        }

        for (int i = 0; i < types.size(); ++i) {
            RelDataType type = types.get(i);
            RelDataTypeFamily family = type.getFamily();

            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == SqlTypeName.NULL)
                continue;

            // Convert Java types; for instance, JavaType(int) becomes INTEGER.
            // Except if all types are either NULL or Java types.
            if (isJavaType(type) && javaCnt + nullCnt < types.size()) {
                final RelDataType originalType = type;
                type = typeName.allowsPrecScale(true, true)
                    ? createSqlType(typeName, type.getPrecision(), type.getScale())
                    : typeName.allowsPrecScale(true, false)
                    ? createSqlType(typeName, type.getPrecision())
                    : createSqlType(typeName);
                type = createTypeWithNullability(type, originalType.isNullable());
            }

            if (resType == null) {
                resType = type;
                if (resType.getSqlTypeName() == SqlTypeName.ROW)
                    return leastRestrictiveStructuredType(types);
            }

            RelDataTypeFamily resFamily = resType.getFamily();
            SqlTypeName resTypeName = resType.getSqlTypeName();

            if (resFamily != family)
                return null;
            if (SqlTypeUtil.inCharOrBinaryFamilies(type)) {
                Charset charset1 = type.getCharset();
                Charset charset2 = resType.getCharset();
                SqlCollation collation1 = type.getCollation();
                SqlCollation collation2 = resType.getCollation();

                final int precision =
                    SqlTypeUtil.maxPrecision(resType.getPrecision(),
                        type.getPrecision());

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                if (SqlTypeUtil.isLob(resType))
                    resType = createSqlType(resType.getSqlTypeName());
                else if (SqlTypeUtil.isLob(type))
                    resType = createSqlType(type.getSqlTypeName());
                else if (SqlTypeUtil.isBoundedVariableWidth(resType)) {
                    resType =
                        createSqlType(
                            resType.getSqlTypeName(),
                            precision);
                } else {
                    // this catch-all case covers type variable, and both fixed

                    SqlTypeName newTypeName = type.getSqlTypeName();

                    if (typeSystem.shouldConvertRaggedUnionTypesToVarying()) {
                        if (resType.getPrecision() != type.getPrecision()) {
                            if (newTypeName == SqlTypeName.CHAR)
                                newTypeName = SqlTypeName.VARCHAR;
                            else if (newTypeName == SqlTypeName.BINARY)
                                newTypeName = SqlTypeName.VARBINARY;
                        }
                    }

                    resType =
                        createSqlType(
                            newTypeName,
                            precision);
                }
                Charset charset = null;
                // TODO:  refine collation combination rules
                SqlCollation collation0 = collation1 != null && collation2 != null
                    ? SqlCollation.getCoercibilityDyadicOperator(collation1, collation2)
                    : null;
                SqlCollation collation = null;
                if ((charset1 != null) || (charset2 != null)) {
                    if (charset1 == null) {
                        charset = charset2;
                        collation = collation2;
                    } else if (charset2 == null) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.equals(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.contains(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else {
                        charset = charset2;
                        collation = collation2;
                    }
                }
                if (charset != null) {
                    resType =
                        createTypeWithCharsetAndCollation(
                            resType,
                            charset,
                            collation0 != null ? collation0 : collation);
                }
            } else if (SqlTypeUtil.isExactNumeric(type)) {
                if (SqlTypeUtil.isExactNumeric(resType)) {
                    // TODO: come up with a cleaner way to support
                    // interval + datetime = datetime++
                    if (types.size() > (i + 1)) {
                        RelDataType type1 = types.get(i + 1);
                        if (SqlTypeUtil.isDatetime(type1)) {
                            resType = type1;
                            return createTypeWithNullability(resType,
                                nullCnt > 0 || nullableCnt > 0);
                        }
                    }
                    if (!type.equals(resType)) {
                        if (!typeName.allowsPrec()
                            && !resTypeName.allowsPrec()) {
                            // use the bigger primitive
                            if (type.getPrecision()
                                > resType.getPrecision())
                                resType = type;
                        } else {
                            // Let the result type have precision (p), scale (s)
                            // and number of whole digits (d) as follows: d =
                            // max(p1 - s1, p2 - s2) s <= max(s1, s2) p = s + d

                            int p1 = resType.getPrecision();
                            int p2 = type.getPrecision();
                            int s1 = resType.getScale();
                            int s2 = type.getScale();
                            final int maxPrecision = typeSystem.getMaxNumericPrecision();
                            final int maxScale = typeSystem.getMaxNumericScale();

                            int dout = Math.max(p1 - s1, p2 - s2);
                            dout =
                                Math.min(
                                    dout,
                                    maxPrecision);

                            int scale = Math.max(s1, s2);
                            scale =
                                Math.min(
                                    scale,
                                    maxPrecision - dout);
                            scale = Math.min(scale, maxScale);

                            int precision = dout + scale;
                            assert precision <= maxPrecision;
                            assert precision > -2
                                || (resType.getSqlTypeName() == SqlTypeName.DECIMAL
                                && precision == 0
                                && scale == 0);

                            resType =
                                createSqlType(
                                    SqlTypeName.DECIMAL,
                                    precision,
                                    scale);
                        }
                    }
                } else if (SqlTypeUtil.isApproximateNumeric(resType)) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    if (SqlTypeUtil.isDecimal(type)) {
                        // Only promote to double for decimal types
                        resType = createDoublePrecisionType();
                    }
                } else
                    return null;
            } else if (SqlTypeUtil.isApproximateNumeric(type)) {
                if (SqlTypeUtil.isApproximateNumeric(resType)) {
                    if (type.getPrecision() > resType.getPrecision())
                        resType = type;
                } else if (SqlTypeUtil.isExactNumeric(resType)) {
                    if (SqlTypeUtil.isDecimal(resType))
                        resType = createDoublePrecisionType();
                    else
                        resType = type;
                } else
                    return null;
            } else if (SqlTypeUtil.isInterval(type)) {
                // TODO: come up with a cleaner way to support
                // interval + datetime = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isDatetime(type1)) {
                        resType = type1;
                        return createTypeWithNullability(resType,
                            nullCnt > 0 || nullableCnt > 0);
                    }
                }

                if (!type.equals(resType)) {
                    // TODO jvs 4-June-2005:  This shouldn't be necessary;
                    // move logic into IntervalSqlType.combine
                    Object type1 = resType;
                    resType =
                        ((IntervalSqlType)resType).combine(
                            this,
                            (IntervalSqlType) type);
                    resType =
                        ((IntervalSqlType)resType).combine(
                            this,
                            (IntervalSqlType) type1);
                }
            } else if (SqlTypeUtil.isDatetime(type)) {
                // TODO: come up with a cleaner way to support
                // datetime +/- interval (or integer) = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isInterval(type1)
                        || SqlTypeUtil.isIntType(type1)) {
                        resType = type;
                        return createTypeWithNullability(resType,
                            nullCnt > 0 || nullableCnt > 0);
                    }
                }
            } else {
                // TODO:  datetime precision details; for now we let
                // leastRestrictiveByCast handle it
                return null;
            }
        }
        if (resType != null && nullableCnt > 0)
            resType = createTypeWithNullability(resType, true);
        return resType;
    }

    /** */
    private RelDataType createDoublePrecisionType() {
        return createSqlType(SqlTypeName.DOUBLE);
    }

    /** {@inheritDoc} */
    @Override public Charset getDefaultCharset() {
        // Use JVM default charset rather then Calcite default charset (ISO-8859-1).
        return Charset.defaultCharset();
    }

    /** */
    private boolean allEquals(List<RelDataType> types) {
        assert types.size() > 1;

        RelDataType first = F.first(types);
        for (int i = 1; i < types.size(); i++) {
            if (!Objects.equals(first, types.get(i)))
                return false;
        }

        return true;
    }
}
