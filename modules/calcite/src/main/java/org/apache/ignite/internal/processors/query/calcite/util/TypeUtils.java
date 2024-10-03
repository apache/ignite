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
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ViewTableImpl;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.UNEXPECTED_ELEMENT_TYPE;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.transform;

/** */
public class TypeUtils {
    /** */
    private static final Set<Type> CONVERTABLE_TYPES = ImmutableSet.of(
        java.util.Date.class,
        java.sql.Date.class,
        java.sql.Time.class,
        java.sql.Timestamp.class,
        LocalDateTime.class,
        LocalDate.class,
        LocalTime.class,
        Duration.class,
        Period.class,
        byte[].class
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
        if (toType.getSqlTypeName() == SqlTypeName.ANY || fromType.getSqlTypeName() == SqlTypeName.ANY) {
            return toType.getClass() != fromType.getClass();
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

        // Currently, RelDataTypeFactoryImpl#CLASS_FAMILIES doesn't consider the byte type as an integer.
        if ((fromType.getSqlTypeName() == SqlTypeName.TINYINT && SqlTypeUtil.isIntType(toType))
            || (toType.getSqlTypeName() == SqlTypeName.TINYINT && SqlTypeUtil.isIntType(fromType)))
            return false;

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
    public static RelDataType sqlType(
        IgniteTypeFactory typeFactory,
        Class<?> cls,
        int precision,
        int scale,
        boolean nullability
    ) {
        RelDataType javaType = typeFactory.createJavaType(cls);

        if (javaType.getSqlTypeName().allowsPrecScale(true, true) &&
            (precision != RelDataType.PRECISION_NOT_SPECIFIED || scale != RelDataType.SCALE_NOT_SPECIFIED)) {
            return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(javaType.getSqlTypeName(), precision, scale), nullability);
        }

        return typeFactory.createTypeWithNullability(sqlType(typeFactory, javaType), nullability);
    }

    /** */
    private static RelDataType sqlType(IgniteTypeFactory typeFactory, RelDataType rowType) {
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
    private static Type getResultClass(
        IgniteTypeFactory typeFactory,
        RelOptSchema schema,
        RelDataType type,
        @Nullable List<String> origin
    ) {
        int maxViewDepth = 100;

        int cnt = 0; // Counter to protect from infinite recursion.

        while (true) {
            if (F.isEmpty(origin))
                return typeFactory.getResultClass(type);

            if (cnt++ >= maxViewDepth)
                throw new UnsupportedOperationException("To many inner views: " + maxViewDepth);

            RelOptTable table = schema.getTableForMember(origin.subList(0, 2));

            assert table != null;

            ViewTableImpl viewTable = table.unwrap(ViewTableImpl.class);

            if (viewTable != null) {
                origin = viewTable.fieldOrigin(origin.get(2));

                continue;
            }

            ColumnDescriptor fldDesc = table.unwrap(TableDescriptor.class).columnDescriptor(origin.get(2));

            assert fldDesc != null;

            return fldDesc.storageType();
        }
    }

    /**
     * @param ectx Execution context.
     * @param resultType Result type.
     */
    public static <Row> Function<Row, Row> resultTypeConverter(ExecutionContext<Row> ectx, RelDataType resultType) {
        assert resultType.isStruct();

        if (hasConvertableFields(resultType)) {
            RowHandler<Row> hnd = ectx.rowHandler();
            List<RelDataType> types = RelOptUtil.getFieldTypeList(resultType);
            RowHandler.RowFactory<Row> factory = hnd.factory(ectx.getTypeFactory(), types);
            List<Function<Object, Object>> converters = transform(types, t -> fieldConverter(ectx, t));
            return r -> {
                Row newRow = factory.create();
                assert hnd.columnCount(newRow) == converters.size();
                assert hnd.columnCount(r) == converters.size();
                for (int i = 0; i < converters.size(); i++)
                    hnd.set(i, newRow, converters.get(i).apply(hnd.get(i, r)));
                return newRow;
            };
        }

        return Function.identity();
    }

    /** */
    private static Function<Object, Object> fieldConverter(ExecutionContext<?> ectx, RelDataType fieldType) {
        Type storageType = ectx.getTypeFactory().getJavaClass(fieldType);

        if (isConvertableType(storageType))
            return v -> fromInternal(ectx, v, storageType);

        return Function.identity();
    }

    /** */
    public static boolean isConvertableType(Type type) {
        return CONVERTABLE_TYPES.contains(type);
    }

    /** */
    public static boolean isConvertableType(RelDataType type) {
        return type instanceof RelDataTypeFactoryImpl.JavaType
                    && isConvertableType(((RelDataTypeFactoryImpl.JavaType)type).getJavaClass());
    }

    /** */
    private static boolean hasConvertableFields(RelDataType resultType) {
        return RelOptUtil.getFieldTypeList(resultType).stream()
            .anyMatch(TypeUtils::isConvertableType);
    }

    /** */
    public static boolean hasPrecision(RelDataType type) {
        // Special case for DECIMAL type without precision and scale specified.
        if (type.getSqlTypeName() == SqlTypeName.DECIMAL &&
            type.getPrecision() == IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL))
            return false;

        return type.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED;
    }

    /** */
    public static boolean hasScale(RelDataType type) {
        // Special case for DECIMAL type without precision and scale specified.
        if (type.getSqlTypeName() == SqlTypeName.DECIMAL &&
            type.getPrecision() == IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL))
            return false;

        return type.getScale() != RelDataType.SCALE_NOT_SPECIFIED;
    }

    /** */
    public static Object toInternal(DataContext ctx, Object val) {
        return val == null ? null : toInternal(ctx, val, val.getClass());
    }

    /** */
    public static Object toInternal(DataContext ctx, Object val, Type storageType) {
        if (val == null)
            return null;
        else if (storageType == java.sql.Date.class)
            return (int)(toLong(ctx, val) / DateTimeUtils.MILLIS_PER_DAY);
        else if (storageType == LocalDate.class)
            return (int)(toLong(ctx, val) / DateTimeUtils.MILLIS_PER_DAY);
        else if (storageType == java.sql.Time.class)
            return (int)(toLong(ctx, val) % DateTimeUtils.MILLIS_PER_DAY);
        else if (storageType == LocalTime.class)
            return (int)(toLong(ctx, val) % DateTimeUtils.MILLIS_PER_DAY);
        else if (storageType == Timestamp.class || storageType == LocalDateTime.class)
            return toLong(ctx, val);
        else if (storageType == java.util.Date.class)
            return toLong(ctx, val);
        else if (storageType == java.util.Date.class)
            return toLong(ctx, val);
        else if (storageType == Duration.class) {
            return TimeUnit.SECONDS.toMillis(((Duration)val).getSeconds())
                + TimeUnit.NANOSECONDS.toMillis(((Duration)val).getNano());
        }
        else if (storageType == Period.class)
            return (int)((Period)val).toTotalMonths();
        else if (storageType == byte[].class)
            return new ByteString((byte[])val);
        else if (val instanceof Number && storageType != val.getClass()) {
            // For dynamic parameters we don't know exact parameter type in compile time. To avoid casting errors in
            // runtime we should convert parameter value to expected type.
            Number num = (Number)val;

            return Byte.class.equals(storageType) || byte.class.equals(storageType) ? SqlFunctions.toByte(num) :
                Short.class.equals(storageType) || short.class.equals(storageType) ? SqlFunctions.toShort(num) :
                Integer.class.equals(storageType) || int.class.equals(storageType) ? SqlFunctions.toInt(num) :
                Long.class.equals(storageType) || long.class.equals(storageType) ? SqlFunctions.toLong(num) :
                Float.class.equals(storageType) || float.class.equals(storageType) ? SqlFunctions.toFloat(num) :
                Double.class.equals(storageType) || double.class.equals(storageType) ? SqlFunctions.toDouble(num) :
                BigDecimal.class.equals(storageType) ? SqlFunctions.toBigDecimal(num) : num;
        }
        else
            return val;
    }

    /** Converts temporal objects to long.
     *
     * @param ctx Data context.
     * @param val Temporal value.
     * @return Millis value.
     */
    private static long toLong(DataContext ctx, Object val) {
        if (val instanceof LocalDateTime)
            return toLong(DateValueUtils.convertToTimestamp((LocalDateTime)val), DataContext.Variable.TIME_ZONE.get(ctx));

        if (val instanceof LocalDate)
            return toLong(DateValueUtils.convertToSqlDate((LocalDate)val), DataContext.Variable.TIME_ZONE.get(ctx));

        if (val instanceof LocalTime)
            return toLong(DateValueUtils.convertToSqlTime((LocalTime)val), DataContext.Variable.TIME_ZONE.get(ctx));

        return toLong((java.util.Date)val, DataContext.Variable.TIME_ZONE.get(ctx));
    }

    /** */
    private static long toLong(java.util.Date val, TimeZone tz) {
        long time = val.getTime();

        return time + tz.getOffset(time);
    }

    /** */
    public static Object fromInternal(DataContext ctx, Object val, Type storageType) {
        if (val == null)
            return null;
        else if (storageType == java.sql.Date.class && val instanceof Integer)
            return new java.sql.Date(fromLocalTs(ctx, (Integer)val * DateTimeUtils.MILLIS_PER_DAY));
        else if (storageType == LocalDate.class && val instanceof Integer)
            return new java.sql.Date(fromLocalTs(ctx, (Integer)val * DateTimeUtils.MILLIS_PER_DAY)).toLocalDate();
        else if (storageType == java.sql.Time.class && val instanceof Integer)
            return new java.sql.Time(fromLocalTs(ctx, (Integer)val));
        else if (storageType == LocalTime.class && val instanceof Integer)
            return Instant.ofEpochMilli((Integer)val).atZone(ZoneOffset.UTC).toLocalTime();
        else if (storageType == Timestamp.class && val instanceof Long)
            return new Timestamp(fromLocalTs(ctx, (Long)val));
        else if (storageType == LocalDateTime.class && val instanceof Long)
            return new Timestamp(fromLocalTs(ctx, (Long)val)).toLocalDateTime();
        else if (storageType == java.util.Date.class && val instanceof Long)
            return new java.util.Date(fromLocalTs(ctx, (Long)val));
        else if (storageType == Duration.class && val instanceof Long)
            return Duration.ofMillis((Long)val);
        else if (storageType == Period.class && val instanceof Integer)
            return Period.of((Integer)val / 12, (Integer)val % 12, 0);
        else if (storageType == byte[].class && val instanceof ByteString)
            return ((ByteString)val).getBytes();
        else
            return val;
    }

    /**
     * Creates a value of required type from the literal.
     */
    public static Object fromLiteral(DataContext ctx, Type storageType, SqlLiteral literal) {
        Object internalVal;

        try {
            storageType = Primitive.box(storageType); // getValueAs() implemented only for boxed classes.

            if (Date.class.equals(storageType)) {
                SqlLiteral literal0 = ((SqlUnknownLiteral)literal).resolve(SqlTypeName.DATE);

                internalVal = literal0.getValueAs(DateString.class).getDaysSinceEpoch();
            }
            else if (Time.class.equals(storageType)) {
                SqlLiteral literal0 = ((SqlUnknownLiteral)literal).resolve(SqlTypeName.TIME);

                internalVal = literal0.getValueAs(TimeString.class).getMillisOfDay();
            }
            else if (Timestamp.class.equals(storageType)) {
                SqlLiteral literal0 = ((SqlUnknownLiteral)literal).resolve(SqlTypeName.TIMESTAMP);

                internalVal = literal0.getValueAs(TimestampString.class).getMillisSinceEpoch();
            }
            else if (Duration.class.equals(storageType)) {
                if (literal instanceof SqlIntervalLiteral &&
                    !literal.getValueAs(SqlIntervalLiteral.IntervalValue.class).getIntervalQualifier().isYearMonth())
                    internalVal = literal.getValueAs(Long.class);
                else
                    throw new IgniteException("Expected DAY-TIME interval literal");
            }
            else if (Period.class.equals(storageType)) {
                if (literal instanceof SqlIntervalLiteral &&
                    literal.getValueAs(SqlIntervalLiteral.IntervalValue.class).getIntervalQualifier().isYearMonth())
                    internalVal = literal.getValueAs(Long.class).intValue();
                else
                    throw new IgniteException("Expected YEAR-MONTH interval literal");
            }
            else if (UUID.class.equals(storageType)) {
                if (literal instanceof SqlCharStringLiteral)
                    internalVal = UUID.fromString(literal.getValueAs(String.class));
                else
                    throw new IgniteException("Expected string literal");
            }
            else {
                if (storageType instanceof Class)
                    internalVal = literal.getValueAs((Class<?>)storageType);
                else
                    throw new IgniteException("Unexpected storage type: " + storageType);
            }
        }
        catch (Throwable t) { // Throwable is requred here, since Calcite throws Assertion error in case of type mismatch.
            throw new IgniteSQLException("Cannot convert literal " + literal + " to type " + storageType,
                UNEXPECTED_ELEMENT_TYPE, t);
        }

        return fromInternal(ctx, internalVal, storageType);
    }

    /** */
    private static long fromLocalTs(DataContext ctx, long ts) {
        TimeZone tz = DataContext.Variable.TIME_ZONE.get(ctx);

        // Taking into account DST, offset can be changed after converting from UTC to time-zone.
        return ts - tz.getOffset(ts - tz.getOffset(ts));
    }

    /**
     * @return RexNode
     */
    public static RexNode toRexLiteral(Object dfltVal, RelDataType type, DataContext ctx, RexBuilder rexBuilder) {
        if (dfltVal instanceof UUID) {
            // There is no internal UUID data type in Calcite, so convert UUID to VARCHAR literal and then cast to UUID.
            dfltVal = dfltVal.toString();
        }
        else
            dfltVal = toInternal(ctx, dfltVal);

        return rexBuilder.makeLiteral(dfltVal, type, true);
    }
}
