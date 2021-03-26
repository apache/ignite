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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.SerializationException;

/**
 * Field accessor to speedup access.
 */
abstract class FieldAccessor {
    /** VarHandle. */
    protected final VarHandle varHandle;

    /** Mode. */
    protected final BinaryMode mode;

    /**
     * Mapped column position in schema.
     * <p>
     * NODE: Do not mix up with column index in {@link Columns} container.
     */
    protected final int colIdx;

    /**
     * Create accessor for the field.
     *
     * @param type Object class.
     * @param col Mapped column.
     * @param colIdx Column index in schema.
     * @return Accessor.
     */
    static FieldAccessor create(Class<?> type, Column col, int colIdx) {
        try {
            final Field field = type.getDeclaredField(col.name());

            if (field.getType().isPrimitive() && col.nullable())
                throw new IllegalArgumentException("Failed to map non-nullable field to nullable column [name=" + field.getName() + ']');

            BinaryMode mode = MarshallerUtil.mode(field.getType());
            final MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());

            VarHandle varHandle = lookup.unreflectVarHandle(field);

            assert mode != null : "Invalid mode for type: " + field.getType();

            switch (mode) {
                case P_BYTE:
                    return new BytePrimitiveAccessor(varHandle, colIdx);

                case P_SHORT:
                    return new ShortPrimitiveAccessor(varHandle, colIdx);

                case P_INT:
                    return new IntPrimitiveAccessor(varHandle, colIdx);

                case P_LONG:
                    return new LongPrimitiveAccessor(varHandle, colIdx);

                case P_FLOAT:
                    return new FloatPrimitiveAccessor(varHandle, colIdx);

                case P_DOUBLE:
                    return new DoublePrimitiveAccessor(varHandle, colIdx);

                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case UUID:
                case BYTE_ARR:
                case BITSET:
                    return new ReferenceFieldAccessor(varHandle, colIdx, mode);

                default:
                    assert false : "Invalid mode " + mode;
            }

            throw new IllegalArgumentException("Failed to create accessor for field [name=" + field.getName() + ']');
        }
        catch (NoSuchFieldException | SecurityException | IllegalAccessException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Create accessor for the field.
     *
     * @param col Column.
     * @param colIdx Column index.
     * @param mode Binary mode.
     * @return Accessor.
     */
    static FieldAccessor createIdentityAccessor(Column col, int colIdx, BinaryMode mode) {
        switch (mode) {
            //  Marshaller read/write object contract methods allowed boxed types only.
            case P_BYTE:
            case P_SHORT:
            case P_INT:
            case P_LONG:
            case P_FLOAT:
            case P_DOUBLE:
                throw new IllegalArgumentException("Primitive key/value types are not possible by API contract.");

            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case BYTE_ARR:
            case BITSET:
                return new IdentityAccessor(colIdx, mode);

            default:
                assert false : "Invalid mode " + mode;
        }

        throw new IllegalArgumentException("Failed to create accessor for column [name=" + col.name() + ']');
    }

    /**
     * Reads value object from row.
     *
     * @param reader Reader.
     * @param colIdx Column index.
     * @param mode Binary read mode.
     * @return Read value object.
     */
    private static Object readRefValue(Row reader, int colIdx, BinaryMode mode) {
        assert reader != null;
        assert colIdx >= 0;

        Object val = null;

        switch (mode) {
            case BYTE:
                val = reader.byteValueBoxed(colIdx);

                break;

            case SHORT:
                val = reader.shortValueBoxed(colIdx);

                break;

            case INT:
                val = reader.intValueBoxed(colIdx);

                break;

            case LONG:
                val = reader.longValueBoxed(colIdx);

                break;

            case FLOAT:
                val = reader.floatValueBoxed(colIdx);

                break;

            case DOUBLE:
                val = reader.doubleValueBoxed(colIdx);

                break;

            case STRING:
                val = reader.stringValue(colIdx);

                break;

            case UUID:
                val = reader.uuidValue(colIdx);

                break;

            case BYTE_ARR:
                val = reader.bytesValue(colIdx);

                break;

            case BITSET:
                val = reader.bitmaskValue(colIdx);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }

        return val;
    }

    /**
     * Writes reference value to row.
     *
     * @param val Value object.
     * @param writer Writer.
     * @param mode Write binary mode.
     */
    private static void writeRefObject(Object val, RowAssembler writer, BinaryMode mode) {
        assert writer != null;

        if (val == null) {
            writer.appendNull();

            return;
        }

        switch (mode) {
            case BYTE:
                writer.appendByte((Byte)val);

                break;

            case SHORT:
                writer.appendShort((Short)val);

                break;

            case INT:
                writer.appendInt((Integer)val);

                break;

            case LONG:
                writer.appendLong((Long)val);

                break;

            case FLOAT:
                writer.appendFloat((Float)val);

                break;

            case DOUBLE:
                writer.appendDouble((Double)val);

                break;

            case STRING:
                writer.appendString((String)val);

                break;

            case UUID:
                writer.appendUuid((UUID)val);

                break;

            case BYTE_ARR:
                writer.appendBytes((byte[])val);

                break;

            case BITSET:
                writer.appendBitmask((BitSet)val);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /**
     * Protected constructor.
     *
     * @param varHandle Field.
     * @param colIdx Column index.
     * @param mode Binary mode;
     */
    protected FieldAccessor(VarHandle varHandle, int colIdx, BinaryMode mode) {
        assert colIdx >= 0;

        this.colIdx = colIdx;
        this.mode = Objects.requireNonNull(mode);
        this.varHandle = Objects.requireNonNull(varHandle);
    }

    /**
     * Protected constructor.
     *
     * @param colIdx Column index.
     * @param mode Binary mode;
     */
    private FieldAccessor(int colIdx, BinaryMode mode) {
        assert colIdx >= 0;
        assert mode != null;

        this.colIdx = colIdx;
        this.mode = mode;
        varHandle = null;
    }

    /**
     * Write object field value to row.
     *
     * @param writer Row writer.
     * @param obj Source object.
     * @throws SerializationException If failed.
     */
    public void write(RowAssembler writer, Object obj) throws SerializationException {
        try {
            write0(writer, obj);
        }
        catch (Exception ex) {
            throw new SerializationException("Failed to write field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Write object field value to row.
     *
     * @param writer Row writer.
     * @param obj Source object.
     */
    protected abstract void write0(RowAssembler writer, Object obj) throws Exception;

    /**
     * Reads value fom row to object field.
     *
     * @param reader Row reader.
     * @param obj Target object.
     * @throws SerializationException If failed.
     */
    public void read(Row reader, Object obj) throws SerializationException {
        try {
            read0(reader, obj);
        }
        catch (Exception ex) {
            throw new SerializationException("Failed to read field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Reads value fom row to object field.
     *
     * @param reader Row reader.
     * @param obj Target object.
     * @throws Exception If failed.
     */
    protected abstract void read0(Row reader, Object obj) throws Exception;

    /**
     * Read value.
     *
     * @param reader Row reader.
     * @return Object.
     */
    public Object read(Row reader) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads object field value.
     *
     * @param obj Object.
     * @return Field value of given object.
     */
    Object value(Object obj) {
        return varHandle.get(Objects.requireNonNull(obj));
    }

    /**
     * Accessor for field of primitive {@code byte} type.
     */
    private static class IdentityAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param colIdx Column index.
         * @param mode Binary mode.
         */
        IdentityAccessor(int colIdx, BinaryMode mode) {
            super(colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            writeRefObject(obj, writer, mode);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            throw new UnsupportedOperationException("Called identity accessor for object field.");
        }

        /** {@inheritDoc} */
        @Override public Object read(Row reader) {
            return readRefValue(reader, colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override Object value(Object obj) {
            return obj;
        }
    }

    /**
     * Accessor for field of primitive {@code byte} type.
     */
    private static class BytePrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        BytePrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(varHandle, colIdx, BinaryMode.P_BYTE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            final byte val = (byte)varHandle.get(obj);

            writer.appendByte(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            final byte val = reader.byteValue(colIdx);

            varHandle.set(obj, val);
        }
    }

    /**
     * Accessor for field of primitive {@code short} type.
     */
    private static class ShortPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        ShortPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(varHandle, colIdx, BinaryMode.P_SHORT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            final short val = (short)varHandle.get(obj);

            writer.appendShort(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            final short val = reader.shortValue(colIdx);

            varHandle.set(obj, val);
        }
    }

    /**
     * Accessor for field of primitive {@code int} type.
     */
    private static class IntPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        IntPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(varHandle, colIdx, BinaryMode.P_INT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            final int val = (int)varHandle.get(obj);

            writer.appendInt(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            final int val = reader.intValue(colIdx);

            varHandle.set(obj, val);
        }
    }

    /**
     * Accessor for field of primitive {@code long} type.
     */
    private static class LongPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        LongPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(varHandle, colIdx, BinaryMode.P_LONG);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            final long val = (long)varHandle.get(obj);

            writer.appendLong(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            final long val = reader.longValue(colIdx);

            varHandle.set(obj, val);
        }
    }

    /**
     * Accessor for field of primitive {@code float} type.
     */
    private static class FloatPrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        FloatPrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(varHandle, colIdx, BinaryMode.P_FLOAT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            final float val = (float)varHandle.get(obj);

            writer.appendFloat(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            final float val = reader.floatValue(colIdx);

            varHandle.set(obj, val);
        }
    }

    /**
     * Accessor for field of primitive {@code double} type.
     */
    private static class DoublePrimitiveAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         */
        DoublePrimitiveAccessor(VarHandle varHandle, int colIdx) {
            super(varHandle, colIdx, BinaryMode.P_DOUBLE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            final double val = (double)varHandle.get(obj);

            writer.appendDouble(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Row reader, Object obj) {
            final double val = reader.doubleValue(colIdx);

            varHandle.set(obj, val);
        }
    }

    /**
     * Accessor for field of reference type.
     */
    private static class ReferenceFieldAccessor extends FieldAccessor {
        /**
         * Constructor.
         *
         * @param varHandle VarHandle.
         * @param colIdx Column index.
         * @param mode Binary mode.
         */
        ReferenceFieldAccessor(VarHandle varHandle, int colIdx, BinaryMode mode) {
            super(varHandle, colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override protected void write0(RowAssembler writer, Object obj) {
            assert obj != null;
            assert writer != null;

            Object val = varHandle.get(obj);

            if (val == null) {
                writer.appendNull();

                return;
            }

            writeRefObject(val, writer, mode);
        }

        /** {@inheritDoc} */
        @Override public void read0(Row reader, Object obj) {
            Object val = readRefValue(reader, colIdx, mode);

            varHandle.set(obj, val);
        }
    }
}
