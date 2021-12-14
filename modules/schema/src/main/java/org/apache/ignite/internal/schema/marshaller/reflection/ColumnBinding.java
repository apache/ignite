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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.table.mapper.TypeConverter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The class represents column-field binding and provides methods to read a column data to an object field, and to write an object field
 * data to a column. The degenerate case, when a whole object itself bind with a single column, is also supported via an identity method
 * handles.
 *
 * @see #createFieldBinding(Column, Class, String, TypeConverter)
 * @see #createIdentityBinding(Column, Class, TypeConverter)
 */
abstract class ColumnBinding {
    private static final MethodHandle NULL_WRITER;

    private static final MethodHandle P_BYTE_READER;
    private static final MethodHandle P_SHORT_READER;
    private static final MethodHandle P_INT_READER;
    private static final MethodHandle P_LONG_READER;
    private static final MethodHandle P_FLOAT_READER;
    private static final MethodHandle P_DOUBLE_READER;

    private static final MethodHandle BYTE_READER;
    private static final MethodHandle SHORT_READER;
    private static final MethodHandle INT_READER;
    private static final MethodHandle LONG_READER;
    private static final MethodHandle FLOAT_READER;
    private static final MethodHandle DOUBLE_READER;
    private static final MethodHandle DECIMAL_READER;
    private static final MethodHandle NUMBER_READER;

    private static final MethodHandle STRING_READER;
    private static final MethodHandle UUID_READER;
    private static final MethodHandle BYTE_ARR_READER;
    private static final MethodHandle BITSET_READER;

    private static final MethodHandle DATE_READER;
    private static final MethodHandle DATETIME_READER;
    private static final MethodHandle TIME_READER;
    private static final MethodHandle TIMESTAMP_READER;

    private static final MethodHandle BYTE_WRITER;
    private static final MethodHandle SHORT_WRITER;
    private static final MethodHandle INT_WRITER;
    private static final MethodHandle LONG_WRITER;
    private static final MethodHandle FLOAT_WRITER;
    private static final MethodHandle DOUBLE_WRITER;
    private static final MethodHandle DECIMAL_WRITER;
    private static final MethodHandle NUMBER_WRITER;

    private static final MethodHandle STRING_WRITER;
    private static final MethodHandle UUID_WRITER;
    private static final MethodHandle BYTE_ARR_WRITER;
    private static final MethodHandle BITSET_WRITER;

    private static final MethodHandle DATE_WRITER;
    private static final MethodHandle DATETIME_WRITER;
    private static final MethodHandle TIME_WRITER;
    private static final MethodHandle TIMESTAMP_WRITER;

    private static final MethodHandle TRANSFORM_BEFORE_WRITE;
    private static final MethodHandle TRANSFORM_AFTER_READ;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();

            NULL_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendNull"));

            P_BYTE_READER = lookup.unreflect(Row.class.getMethod("byteValue", int.class));
            P_SHORT_READER = lookup.unreflect(Row.class.getMethod("shortValue", int.class));
            P_INT_READER = lookup.unreflect(Row.class.getMethod("intValue", int.class));
            P_LONG_READER = lookup.unreflect(Row.class.getMethod("longValue", int.class));
            P_FLOAT_READER = lookup.unreflect(Row.class.getMethod("floatValue", int.class));
            P_DOUBLE_READER = lookup.unreflect(Row.class.getMethod("doubleValue", int.class));

            BYTE_READER = lookup.unreflect(Row.class.getMethod("byteValueBoxed", int.class));
            SHORT_READER = lookup.unreflect(Row.class.getMethod("shortValueBoxed", int.class));
            INT_READER = lookup.unreflect(Row.class.getMethod("intValueBoxed", int.class));
            LONG_READER = lookup.unreflect(Row.class.getMethod("longValueBoxed", int.class));
            FLOAT_READER = lookup.unreflect(Row.class.getMethod("floatValueBoxed", int.class));
            DOUBLE_READER = lookup.unreflect(Row.class.getMethod("doubleValueBoxed", int.class));

            NUMBER_READER = lookup.unreflect(Row.class.getMethod("numberValue", int.class));
            DECIMAL_READER = lookup.unreflect(Row.class.getMethod("decimalValue", int.class));

            STRING_READER = lookup.unreflect(Row.class.getMethod("stringValue", int.class));
            UUID_READER = lookup.unreflect(Row.class.getMethod("uuidValue", int.class));
            BYTE_ARR_READER = lookup.unreflect(Row.class.getMethod("bytesValue", int.class));
            BITSET_READER = lookup.unreflect(Row.class.getMethod("bitmaskValue", int.class));

            DATE_READER = lookup.unreflect(Row.class.getMethod("dateValue", int.class));
            TIME_READER = lookup.unreflect(Row.class.getMethod("timeValue", int.class));
            TIMESTAMP_READER = lookup.unreflect(Row.class.getMethod("timestampValue", int.class));
            DATETIME_READER = lookup.unreflect(Row.class.getMethod("dateTimeValue", int.class));

            BYTE_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendByte", byte.class));
            SHORT_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendShort", short.class));
            INT_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendInt", int.class));
            LONG_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendLong", long.class));
            FLOAT_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendFloat", float.class));
            DOUBLE_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendDouble", double.class));
            STRING_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendString", String.class));

            UUID_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendUuid", UUID.class));
            BYTE_ARR_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendBytes", byte[].class));
            BITSET_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendBitmask", BitSet.class));
            NUMBER_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendNumber", BigInteger.class));
            DECIMAL_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendDecimal", BigDecimal.class));

            DATE_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendDate", LocalDate.class));
            TIME_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendTime", LocalTime.class));
            TIMESTAMP_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendTimestamp", Instant.class));
            DATETIME_WRITER = lookup.unreflect(RowAssembler.class.getMethod("appendDateTime", LocalDateTime.class));

            TRANSFORM_AFTER_READ = lookup.unreflect(TypeConverter.class.getMethod("toObjectType", Object.class));
            TRANSFORM_BEFORE_WRITE = lookup.unreflect(TypeConverter.class.getMethod("toColumnType", Object.class));
        } catch (IllegalAccessException | NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Get method for a bind field (if applicable, otherwise {@code null}). */
    protected final MethodHandle getterHnd;

    /** Set method for a bind field (if applicable, otherwise {@code null}). */
    protected final MethodHandle setterHnd;

    /** {@link Row} read method for a bind column. */
    protected final MethodHandle readerHnd;

    /** {@link RowAssembler} write method for a bind column. */
    protected final MethodHandle writerHnd;

    /**
     * Mapped column position in the schema.
     *
     * <p>NODE: Do not mix up with column index in {@link Columns} container.
     */
    protected final int colIdx;

    /**
     * Creates a dummy binder that materializes column default value on write and ignores the column on read.
     */
    static ColumnBinding unmappedFieldBinding(Column col) {
        return new UnmappedFieldBinding(col);
    }

    /**
     * Binds the individual object field with a column.
     *
     * @param col       A column the field is mapped to.
     * @param type      Object class.
     * @param fldName   Object field name.
     * @param converter Type converter or {@code null}.
     * @return Column to field binding.
     */
    static ColumnBinding createFieldBinding(Column col, Class<?> type, @NotNull String fldName, @Nullable TypeConverter<?, ?> converter) {
        try {
            final Field field = type.getDeclaredField(fldName);

            VarHandle varHandle = MethodHandles.privateLookupIn(type, MethodHandles.lookup()).unreflectVarHandle(field);

            if (varHandle.varType().isPrimitive() && col.nullable()) {
                throw new IllegalArgumentException(String.format("Failed to map non-nullable field to nullable column: columnName=%s, "
                                                                         + "fieldName=%s, class=%s", col.name(), fldName, type.getName()));
            }

            return create(
                    col,
                    varHandle.varType(),
                    varHandle.toMethodHandle(VarHandle.AccessMode.GET),
                    varHandle.toMethodHandle(VarHandle.AccessMode.SET),
                    converter
            );
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Binds a column with an object of given type.
     *
     * @param col       Column.
     * @param type      Object type.
     * @param converter Type converter or {@code null}.
     * @return Column to object binding.
     */
    static @NotNull ColumnBinding createIdentityBinding(Column col, Class<?> type, @Nullable TypeConverter<?, ?> converter) {
        final BinaryMode mode = MarshallerUtil.mode(type);

        if (mode.typeSpec() != col.type().spec()) {
            throw new SchemaMismatchException(
                    String.format("Object can't be mapped to a column of incompatible type: columnType=%s, mappedType=%s",
                            col.type().spec(), type.getName()));
        }

        final MethodHandle identityHandle = MethodHandles.identity(type);
        return create(col, type, identityHandle, identityHandle, converter);
    }

    /**
     * Binds a column with an object`s field of given type.
     *
     * @param col          Column.
     * @param type         Object type.
     * @param getterHandle Field getter handle.
     * @param setterHandle Field setter handle.
     * @param converter    Type converter or {@code null}.
     * @return Column binding.
     */
    private static @NotNull ColumnBinding create(
            Column col,
            Class<?> type,
            MethodHandle getterHandle,
            MethodHandle setterHandle,
            @Nullable TypeConverter<?, ?> converter
    ) {
        final int colIdx = col.schemaIndex();

        switch (MarshallerUtil.mode(type)) {
            case P_BYTE:
                return create(colIdx, getterHandle, setterHandle, P_BYTE_READER, BYTE_WRITER, converter);
            case P_SHORT:
                return create(colIdx, getterHandle, setterHandle, P_SHORT_READER, SHORT_WRITER, converter);
            case P_INT:
                return create(colIdx, getterHandle, setterHandle, P_INT_READER, INT_WRITER, converter);
            case P_LONG:
                return create(colIdx, getterHandle, setterHandle, P_LONG_READER, LONG_WRITER, converter);
            case P_FLOAT:
                return create(colIdx, getterHandle, setterHandle, P_FLOAT_READER, FLOAT_WRITER, converter);
            case P_DOUBLE:
                return create(colIdx, getterHandle, setterHandle, P_DOUBLE_READER, DOUBLE_WRITER, converter);
            case BYTE:
                return create(colIdx, getterHandle, setterHandle, BYTE_READER, BYTE_WRITER, converter);
            case SHORT:
                return create(colIdx, getterHandle, setterHandle, SHORT_READER, SHORT_WRITER, converter);
            case INT:
                return create(colIdx, getterHandle, setterHandle, INT_READER, INT_WRITER, converter);
            case LONG:
                return create(colIdx, getterHandle, setterHandle, LONG_READER, LONG_WRITER, converter);
            case FLOAT:
                return create(colIdx, getterHandle, setterHandle, FLOAT_READER, FLOAT_WRITER, converter);
            case DOUBLE:
                return create(colIdx, getterHandle, setterHandle, DOUBLE_READER, DOUBLE_WRITER, converter);
            case STRING:
                return create(colIdx, getterHandle, setterHandle, STRING_READER, STRING_WRITER, converter);
            case UUID:
                return create(colIdx, getterHandle, setterHandle, UUID_READER, UUID_WRITER, converter);
            case BYTE_ARR:
                return create(colIdx, getterHandle, setterHandle, BYTE_ARR_READER, BYTE_ARR_WRITER, converter);
            case BITSET:
                return create(colIdx, getterHandle, setterHandle, BITSET_READER, BITSET_WRITER, converter);
            case NUMBER:
                return create(colIdx, getterHandle, setterHandle, NUMBER_READER, NUMBER_WRITER, converter);
            case DECIMAL:
                return create(colIdx, getterHandle, setterHandle, DECIMAL_READER, DECIMAL_WRITER, converter);
            case TIME:
                return create(colIdx, getterHandle, setterHandle, TIME_READER, TIME_WRITER, converter);
            case DATE:
                return create(colIdx, getterHandle, setterHandle, DATE_READER, DATE_WRITER, converter);
            case DATETIME:
                return create(colIdx, getterHandle, setterHandle, DATETIME_READER, DATETIME_WRITER, converter);
            case TIMESTAMP:
                return create(colIdx, getterHandle, setterHandle, TIMESTAMP_READER, TIMESTAMP_WRITER, converter);
            case POJO:
                if (converter == null) {
                    throw new IllegalArgumentException(String.format("Failed to bind column to a POJO class/field without converter:"
                                                                             + " columnName=%s, targetType=%s", col.name(), type));
                }

                return create(colIdx, getterHandle, setterHandle, BYTE_ARR_READER, BYTE_ARR_WRITER, converter);
            default:
                throw new IllegalArgumentException(
                        String.format("Failed to bind column to a class/field: columnName=%s, targetType=%s", col.name(), type));
        }
    }

    /**
     * Creates binding.
     *
     * @param colIdx       Column index.
     * @param getterHandle Field getter handle.
     * @param setterHandle Field setter handle.
     * @param readerHandle Column reader handle.
     * @param writerHandle Column writer handle.
     * @param converter    Type converter or {@code null}.
     * @return Column binding.
     */
    @NotNull
    private static ColumnBinding create(int colIdx, MethodHandle getterHandle, MethodHandle setterHandle, MethodHandle readerHandle,
            MethodHandle writerHandle, @Nullable TypeConverter<?, ?> converter) {
        if (converter != null) {
            return new TransformingBinding(colIdx, getterHandle, setterHandle, readerHandle, writerHandle, converter);
        } else if (readerHandle.type().returnType().isPrimitive()) {
            return new PrimitiveFieldBinding(colIdx, getterHandle, setterHandle, readerHandle, writerHandle);
        } else {
            return new DefaultBinding(colIdx, getterHandle, setterHandle, readerHandle, writerHandle);
        }
    }

    /**
     * Created column binding.
     *
     * @param colIdx    Column index.
     * @param getterMtd Field getter method handler.
     * @param setterMtd Field setter method handler.
     * @param readerHnd Column reader method handler.
     * @param writerHnd Column writer method handler.
     */
    private ColumnBinding(int colIdx, MethodHandle getterMtd, MethodHandle setterMtd, MethodHandle readerHnd, MethodHandle writerHnd) {
        assert colIdx >= 0;
        this.colIdx = colIdx;

        this.getterHnd = getterMtd;
        this.setterHnd = setterMtd;

        this.writerHnd = writerHnd;
        this.readerHnd = readerHnd;
    }


    /**
     * Write object field value to row.
     *
     * @param writer Row writer.
     * @param obj    Source object.
     * @throws MarshallerException If failed.
     */
    public void write(RowAssembler writer, Object obj) throws MarshallerException {
        try {
            write0(writer, obj);
        } catch (Throwable ex) {
            throw new MarshallerException("Failed to write field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Reads value fom row to object field.
     *
     * @param reader Row reader.
     * @param obj    Target object.
     * @throws MarshallerException If failed.
     */
    public void read(Row reader, Object obj) throws MarshallerException {
        try {
            read0(reader, obj);
        } catch (Throwable ex) {
            throw new MarshallerException("Failed to read field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Write object field value to row.
     *
     * @param writer Row writer.
     * @param obj    Source object.
     * @throws Exception If write failed.
     */
    protected abstract void write0(RowAssembler writer, Object obj) throws Throwable;

    /**
     * Reads value fom row to object field.
     *
     * @param reader Row reader.
     * @param obj    Target object.
     * @throws Exception If failed.
     */
    protected abstract void read0(Row reader, Object obj) throws Throwable;

    /**
     * Read an object from a row.
     *
     * @param reader Row reader.
     * @return Object.
     */
    public Object columnValue(Row reader) throws MarshallerException {
        try {
            return this.readerHnd.invoke(reader, colIdx);
        } catch (Throwable ex) {
            throw new MarshallerException("Failed to read column [id=" + colIdx + ']', ex);
        }
    }


    /**
     * Reads object field value.
     *
     * @param obj Object.
     * @return Field value of given object.
     */
    Object value(Object obj) throws MarshallerException {
        try {
            return getterHnd.invoke(Objects.requireNonNull(obj));
        } catch (Throwable ex) {
            throw new MarshallerException("Failed to read field for column: [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Stubbed accessor for unused columns writes default column value, and ignore value on read access.
     */
    private static class UnmappedFieldBinding extends ColumnBinding {
        /** Column. */
        private final Column col;

        /**
         * Constructor.
         *
         * @param col Column.
         */
        UnmappedFieldBinding(Column col) {
            super(col.schemaIndex(), null, null, null, null);
            this.col = col;
        }

        /** {@inheritDoc} */
        @Override
        protected void read0(Row reader, Object obj) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override
        protected void write0(RowAssembler writer, Object obj) {
            RowAssembler.writeValue(writer, col, col.defaultValue());
        }

        /** {@inheritDoc} */
        @Override
        Object value(Object obj) {
            return col.defaultValue();
        }
    }

    /**
     * Binding for an object or for a reference typed field.
     */
    private static class DefaultBinding extends ColumnBinding {

        /**
         * Create default binding for an object or reference field.
         *
         * @param colIdx    Column index.
         * @param getterMtd Field getter method handler (or identity method handler if not applicable).
         * @param setterMtd Field setter method handler (or identity method handler if not applicable).
         * @param readerHnd Column reader method handler.
         * @param writerHnd Column writer method handler.
         */
        DefaultBinding(int colIdx, MethodHandle getterMtd, MethodHandle setterMtd, MethodHandle readerHnd, MethodHandle writerHnd) {
            super(colIdx, getterMtd, setterMtd, readerHnd, writerHnd);
        }

        /** {@inheritDoc} */
        @Override
        protected void write0(RowAssembler writer, Object obj) throws Throwable {
            obj = getterHnd.invoke(obj);

            if (obj == null) {
                NULL_WRITER.invoke(writer);
            } else {
                this.writerHnd.invoke(writer, obj);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void read0(Row reader, Object obj) throws Throwable {
            setterHnd.invoke(obj, this.readerHnd.invoke(reader, colIdx));
        }
    }

    /**
     * Binding for a field of primitive type.
     */
    private static class PrimitiveFieldBinding extends ColumnBinding {
        /**
         * Create primitive field binding.
         *
         * @param colIdx    Column index.
         * @param getterMtd Field getter method handler.
         * @param setterMtd Field setter method handler.
         * @param readerHnd Column reader method handler.
         * @param writerHnd Column writer method handler.
         */
        PrimitiveFieldBinding(int colIdx, MethodHandle getterMtd, MethodHandle setterMtd, MethodHandle readerHnd, MethodHandle writerHnd) {
            super(colIdx, getterMtd, setterMtd, readerHnd, writerHnd);
        }

        /** {@inheritDoc} */
        @Override
        protected void write0(RowAssembler writer, Object obj) throws Throwable {
            assert obj != null;

            this.writerHnd.invoke(writer, getterHnd.invoke(obj));
        }

        /** {@inheritDoc} */
        @Override
        public void read0(Row reader, Object obj) throws Throwable {
            assert obj != null;

            setterHnd.invoke(obj, this.readerHnd.invoke(reader, colIdx));
        }
    }

    /**
     * Binding implies an additional data transformation on before write/after read a column.
     */
    private static class TransformingBinding extends ColumnBinding {
        private final MethodHandle afterReadHnd;

        private final MethodHandle beforeWriteHnd;

        /**
         * Create transforming binding.
         *
         * @param colIdx    Column index.
         * @param getterMtd Field getter method handler.
         * @param setterMtd Field setter method handler.
         * @param readerHnd Column reader method handler.
         * @param writerHnd Column writer method handler.
         */
        TransformingBinding(int colIdx, MethodHandle getterMtd, MethodHandle setterMtd, MethodHandle readerHnd,
                MethodHandle writerHnd,
                TypeConverter<?, ?> interceptor) {
            super(colIdx, getterMtd, setterMtd, readerHnd, writerHnd);

            afterReadHnd = TRANSFORM_AFTER_READ.bindTo(interceptor);
            beforeWriteHnd = TRANSFORM_BEFORE_WRITE.bindTo(interceptor);
        }

        /** {@inheritDoc} */
        @Override
        protected void write0(RowAssembler writer, Object obj) throws Throwable {
            assert obj != null;

            Object val = getterHnd.invoke(obj);

            val = beforeWriteHnd.invoke(val);

            if (val == null) {
                NULL_WRITER.invoke(writer);
            } else {
                this.writerHnd.invoke(writer, val);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void read0(Row reader, Object obj) throws Throwable {
            assert obj != null;

            Object val = this.readerHnd.invoke(reader, colIdx);

            val = afterReadHnd.invoke(val);

            this.setterHnd.invoke(obj, val);
        }

        /** {@inheritDoc} */
        @Override
        public Object columnValue(Row reader) throws MarshallerException {
            try {
                Object val = this.readerHnd.invoke(reader, colIdx);

                return afterReadHnd.invoke(val);
            } catch (Throwable e) {
                throw new MarshallerException(e);
            }
        }
    }
}
