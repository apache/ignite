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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.nativeTypeToClass;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.transform;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/** */
public class TypeUtils {
    /** */
    private static final EnumSet<SqlTypeName> CONVERTABLE_SQL_TYPES = EnumSet.of(
        SqlTypeName.DATE,
        SqlTypeName.TIME,
        SqlTypeName.TIMESTAMP
    );

    /** */
    private static final Set<Type> CONVERTABLE_TYPES = Set.of(
        java.util.Date.class,
        java.sql.Date.class,
        java.sql.Time.class,
        java.sql.Timestamp.class
    );

    /** */
    public static RelDataType combinedRowType(IgniteTypeFactory typeFactory, RelDataType... types) {

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        Set<String> names = new HashSet<>();

        for (RelDataType type : types) {
            for (RelDataTypeField field : type.getFieldList()) {
                int idx = 0;
                String fieldName = field.getName();

                while (!names.add(fieldName))
                    fieldName = field.getName() + idx++;

                builder.add(fieldName, field.getType());
            }
        }

        return builder.build();
    }

    /** */
    public static boolean needCast(RelDataTypeFactory factory, RelDataType fromType, RelDataType toType) {
        // This prevents that we cast a JavaType to normal RelDataType.
        if (fromType instanceof RelDataTypeFactoryImpl.JavaType
            && toType.getSqlTypeName() == fromType.getSqlTypeName()) {
            return false;
        }

        // Do not make a cast when we don't know specific type (ANY) of the origin node.
        if (toType.getSqlTypeName() == SqlTypeName.ANY
            || fromType.getSqlTypeName() == SqlTypeName.ANY) {
            return false;
        }

        // No need to cast between char and varchar.
        if (SqlTypeUtil.isCharacter(toType) && SqlTypeUtil.isCharacter(fromType))
            return false;

        // No need to cast if the source type precedence list
        // contains target type. i.e. do not cast from
        // tinyint to int or int to bigint.
        if (fromType.getPrecedenceList().containsType(toType)
            && SqlTypeUtil.isIntType(fromType)
            && SqlTypeUtil.isIntType(toType)) {
            return false;
        }

        // Implicit type coercion does not handle nullability.
        if (SqlTypeUtil.equalSansNullability(factory, fromType, toType))
            return false;

        // Should keep sync with rules in SqlTypeCoercionRule.
        assert SqlTypeUtil.canCastFrom(toType, fromType, true);

        return true;
    }

    /** */
    @NotNull public static RelDataType createRowType(@NotNull IgniteTypeFactory typeFactory, @NotNull Class<?>... fields) {
        List<RelDataType> types = Arrays.stream(fields)
            .map(typeFactory::createJavaType)
            .collect(Collectors.toList());

        return createRowType(typeFactory, types, "$F");
    }

    /** */
    @NotNull public static RelDataType createRowType(@NotNull IgniteTypeFactory typeFactory, @NotNull RelDataType... fields) {
        List<RelDataType> types = Arrays.asList(fields);

        return createRowType(typeFactory, types, "$F");
    }

    /** */
    private static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields, String namePreffix) {
        List<String> names = IntStream.range(0, fields.size())
            .mapToObj(ord -> namePreffix + ord)
            .collect(Collectors.toList());

        return typeFactory.createStructType(fields, names);
    }

    /** */
    public static RelDataType sqlType(IgniteTypeFactory typeFactory, RelDataType rowType) {
        if (!rowType.isStruct())
            return typeFactory.toSql(rowType);

        return typeFactory.createStructType(
            transform(rowType.getFieldList(),
                f -> Pair.of(f.getName(), sqlType(typeFactory, f.getType()))));
    }

    /**
     * @param schema Schema.
     * @param sqlType Logical row type.
     * @param origins Columns origins.
     * @return Result type.
     */
    public static RelDataType getResultType(IgniteTypeFactory typeFactory, RelOptSchema schema, RelDataType sqlType,
        @Nullable List<List<String>> origins) {
        assert origins == null || origins.size() == sqlType.getFieldCount();

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
        List<RelDataTypeField> fields = sqlType.getFieldList();

        for (int i = 0; i < sqlType.getFieldCount(); i++) {
            List<String> origin = origins == null ? null : origins.get(i);
            b.add(fields.get(i).getName(), typeFactory.createType(
                getResultClass(typeFactory, schema, fields.get(i).getType(), origin)));
        }

        return b.build();
    }

    /**
     * @param schema Schema.
     * @param type Logical column type.
     * @param origin Column origin.
     * @return Result type.
     */
    private static Type getResultClass(IgniteTypeFactory typeFactory, RelOptSchema schema, RelDataType type,
        @Nullable List<String> origin) {
        if (nullOrEmpty(origin))
            return typeFactory.getResultClass(type);

        RelOptTable table = schema.getTableForMember(origin.subList(0, 2));

        assert table != null;

        ColumnDescriptor fldDesc = table.unwrap(TableDescriptor.class).columnDescriptor(origin.get(2));

        assert fldDesc != null;

        return nativeTypeToClass(fldDesc.storageType());
    }

    /** */
    public static <Row> Function<Row, Row> resultTypeConverter(ExecutionContext<Row> ectx, RelDataType resultType) {
        assert resultType.isStruct();

        if (hasConvertableFields(resultType)) {
            RowHandler<Row> handler = ectx.rowHandler();
            List<RelDataType> types = RelOptUtil.getFieldTypeList(resultType);
            RowHandler.RowFactory<Row> factory = handler.factory(ectx.getTypeFactory(), types);
            List<Function<Object, Object>> converters = transform(types, t -> fieldConverter(ectx, t));
            return r -> {
                Row newRow = factory.create();
                assert handler.columnCount(newRow) == converters.size();
                assert handler.columnCount(r) == converters.size();
                for (int i = 0; i < converters.size(); i++)
                    handler.set(i, newRow, converters.get(i).apply(handler.get(i, r)));
                return newRow;
            };
        }

        return Function.identity();
    }

    /** */
    private static Function<Object, Object> fieldConverter(ExecutionContext<?> ectx, RelDataType fieldType) {
        if (CONVERTABLE_SQL_TYPES.contains(fieldType.getSqlTypeName())) {
            Type storageType = ectx.getTypeFactory().getJavaClass(fieldType);
            return v -> fromInternal(ectx, v, storageType);
        }
        return Function.identity();
    }

    /** */
    public static boolean isConvertableType(Type type) {
        return CONVERTABLE_TYPES.contains(type);
    }

    /** */
    public static boolean isConvertableType(RelDataType type) {
        return CONVERTABLE_SQL_TYPES.contains(type.getSqlTypeName());
    }

    /** */
    private static boolean hasConvertableFields(RelDataType resultType) {
        return RelOptUtil.getFieldTypeList(resultType).stream()
            .anyMatch(t -> CONVERTABLE_SQL_TYPES.contains(t.getSqlTypeName()));
    }

    /** */
    public static Object toInternal(ExecutionContext<?> ectx, Object val) {
        return val == null ? null : toInternal(ectx, val, val.getClass());
    }

    /** */
    public static Object toInternal(ExecutionContext<?> ectx, Object val, Type storageType) {
        if (val == null)
            return null;
        else if (storageType == java.sql.Date.class)
            return (int)(SqlFunctions.toLong((java.util.Date)val, DataContext.Variable.TIME_ZONE.get(ectx)) / DateTimeUtils.MILLIS_PER_DAY);
        else if (storageType == java.sql.Time.class)
            return (int)(SqlFunctions.toLong((java.util.Date)val, DataContext.Variable.TIME_ZONE.get(ectx)) % DateTimeUtils.MILLIS_PER_DAY);
        else if (storageType == Timestamp.class)
            return SqlFunctions.toLong((java.util.Date)val, DataContext.Variable.TIME_ZONE.get(ectx));
        else if (storageType == java.util.Date.class)
            return SqlFunctions.toLong((java.util.Date)val, DataContext.Variable.TIME_ZONE.get(ectx));
        else
            return val;
    }

    /** */
    public static Object fromInternal(ExecutionContext<?> ectx, Object val, Type storageType) {
        if (val == null)
            return null;
        else if (storageType == java.sql.Date.class && val instanceof Integer) {
            final long t = (Integer)val * DateTimeUtils.MILLIS_PER_DAY;
            return new java.sql.Date(t - DataContext.Variable.TIME_ZONE.<TimeZone>get(ectx).getOffset(t));
        }
        else if (storageType == java.sql.Time.class && val instanceof Integer)
            return new java.sql.Time((Integer)val - DataContext.Variable.TIME_ZONE.<TimeZone>get(ectx).getOffset((Integer)val));
        else if (storageType == Timestamp.class && val instanceof Long)
            return new Timestamp((Long)val - DataContext.Variable.TIME_ZONE.<TimeZone>get(ectx).getOffset((Long)val));
        else if (storageType == java.util.Date.class && val instanceof Long)
            return new java.util.Date((Long)val - DataContext.Variable.TIME_ZONE.<TimeZone>get(ectx).getOffset((Long)val));
        else
            return val;
    }
}
