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

package org.apache.ignite.internal.binary;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Field accessor to speedup access.
 */
abstract class BinaryFieldAccessor {
    /** Field ID. */
    protected final int id;

    /** Field name */
    protected final String name;

    /** Mode. */
    protected final BinaryWriteMode mode;

    /** Offset. Used for primitive fields, only. */
    protected final long offset;

    /** Target field. Must be not null for non-primitive fields. */
    protected final @Nullable Field field;

    /** Dynamic accessor flag. */
    protected final boolean dynamic;

    /**
     * Create accessor for the field.
     *
     * @param field Field.
     * @param id FIeld ID.
     * @return Accessor.
     */
    public static BinaryFieldAccessor create(Field field, int id) {
        BinaryWriteMode mode = BinaryUtils.mode(field.getType());

        switch (mode) {
            case P_BYTE:
                return new BytePrimitiveAccessor(field, id);

            case P_BOOLEAN:
                return new BooleanPrimitiveAccessor(field, id);

            case P_SHORT:
                return new ShortPrimitiveAccessor(field, id);

            case P_CHAR:
                return new CharPrimitiveAccessor(field, id);

            case P_INT:
                return new IntPrimitiveAccessor(field, id);

            case P_LONG:
                return new LongPrimitiveAccessor(field, id);

            case P_FLOAT:
                return new FloatPrimitiveAccessor(field, id);

            case P_DOUBLE:
                return new DoublePrimitiveAccessor(field, id);

            case BYTE:
            case BOOLEAN:
            case SHORT:
            case CHAR:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case STRING:
            case UUID:
            case DATE:
            case TIMESTAMP:
            case TIME:
            case BYTE_ARR:
            case SHORT_ARR:
            case INT_ARR:
            case LONG_ARR:
            case FLOAT_ARR:
            case DOUBLE_ARR:
            case CHAR_ARR:
            case BOOLEAN_ARR:
            case DECIMAL_ARR:
            case STRING_ARR:
            case UUID_ARR:
            case DATE_ARR:
            case TIMESTAMP_ARR:
            case TIME_ARR:
            case ENUM_ARR:
            case OBJECT_ARR:
            case BINARY_OBJ:
            case BINARY:
                return new DefaultFinalClassAccessor(field, id, mode, false);

            default:
                return new DefaultFinalClassAccessor(field, id, mode, !CommonUtils.isFinal(field.getType()));
        }
    }

    /**
     * Protected constructor.
     *
     * @param id Field ID.
     * @param mode Mode;
     */
    protected BinaryFieldAccessor(Field field, int id, BinaryWriteMode mode, long offset, Field fld, boolean dynamic) {
        assert field != null;
        assert id != 0;
        assert mode != null;

        this.name = field.getName();
        this.id = id;
        this.mode = mode;
        this.offset = offset;
        this.field = fld;
        this.dynamic = dynamic;
    }

    /**
     * Get mode.
     *
     * @return Mode.
     */
    public BinaryWriteMode mode() {
        return mode;
    }

    /**
     * Write field.
     *
     * @param obj Object.
     * @param writer Writer.
     * @throws BinaryObjectException If failed.
     */
    public void write(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
        try {
            write0(obj, writer);
        }
        catch (UnregisteredClassException | UnregisteredBinaryTypeException ex) {
            throw ex;
        }
        catch (Exception ex) {
            if (S.includeSensitive() && !F.isEmpty(name))
                throw new BinaryObjectException("Failed to write field [name=" + name + ']', ex);
            else
                throw new BinaryObjectException("Failed to write field [id=" + id + ']', ex);
        }
    }

    /**
     * Write field.
     *
     * @param obj Object.
     * @param writer Writer.
     * @throws BinaryObjectException If failed.
     */
    protected abstract void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException;

    /**
     * Base primitive field accessor.
     */
    private abstract static class AbstractPrimitiveAccessor extends BinaryFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param id Field ID.
         * @param mode Mode.
         */
        protected AbstractPrimitiveAccessor(Field field, int id, BinaryWriteMode mode) {
            super(field, id, mode, GridUnsafe.objectFieldOffset(field), null, false);
        }
    }

    /**
     * Byte field accessor.
     */
    private static class BytePrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public BytePrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_BYTE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            byte val = GridUnsafe.getByteField(obj, offset);

            writer.writeByteFieldPrimitive(val);
        }
    }

    /**
     * Boolean field accessor.
     */
    private static class BooleanPrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public BooleanPrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_BOOLEAN);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            boolean val = GridUnsafe.getBooleanField(obj, offset);

            writer.writeBooleanFieldPrimitive(val);
        }
    }

    /**
     * Short field accessor.
     */
    private static class ShortPrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public ShortPrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_SHORT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            short val = GridUnsafe.getShortField(obj, offset);

            writer.writeShortFieldPrimitive(val);
        }
    }

    /**
     * Char field accessor.
     */
    private static class CharPrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public CharPrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_CHAR);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            char val = GridUnsafe.getCharField(obj, offset);

            writer.writeCharFieldPrimitive(val);
        }
    }

    /**
     * Int field accessor.
     */
    private static class IntPrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public IntPrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_INT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            int val = GridUnsafe.getIntField(obj, offset);

            writer.writeIntFieldPrimitive(val);
        }
    }

    /**
     * Long field accessor.
     */
    private static class LongPrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public LongPrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_LONG);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            long val = GridUnsafe.getLongField(obj, offset);

            writer.writeLongFieldPrimitive(val);
        }
    }

    /**
     * Float field accessor.
     */
    private static class FloatPrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public FloatPrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_FLOAT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            float val = GridUnsafe.getFloatField(obj, offset);

            writer.writeFloatFieldPrimitive(val);
        }
    }

    /**
     * Double field accessor.
     */
    private static class DoublePrimitiveAccessor extends AbstractPrimitiveAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         */
        public DoublePrimitiveAccessor(Field field, int id) {
            super(field, id, BinaryWriteMode.P_DOUBLE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            writer.writeFieldIdNoSchemaUpdate(id);

            double val = GridUnsafe.getDoubleField(obj, offset);

            writer.writeDoubleFieldPrimitive(val);
        }
    }

    /**
     * Default accessor.
     */
    private static class DefaultFinalClassAccessor extends BinaryFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param id Field ID.
         * @param mode Mode.
         */
        DefaultFinalClassAccessor(Field field, int id, BinaryWriteMode mode, boolean dynamic) {
            super(field, id, mode, -1L, field, dynamic);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
            assert obj != null;
            assert writer != null;

            writer.writeFieldIdNoSchemaUpdate(id);

            Object val;

            try {
                val = field.get(obj);
            }
            catch (IllegalAccessException e) {
                throw new BinaryObjectException("Failed to get value for field: " + field, e);
            }

            switch (mode(val)) {
                case BYTE:
                    writer.writeByteField((Byte)val);

                    break;

                case SHORT:
                    writer.writeShortField((Short)val);

                    break;

                case INT:
                    writer.writeIntField((Integer)val);

                    break;

                case LONG:
                    writer.writeLongField((Long)val);

                    break;

                case FLOAT:
                    writer.writeFloatField((Float)val);

                    break;

                case DOUBLE:
                    writer.writeDoubleField((Double)val);

                    break;

                case CHAR:
                    writer.writeCharField((Character)val);

                    break;

                case BOOLEAN:
                    writer.writeBooleanField((Boolean)val);

                    break;

                case DECIMAL:
                    writer.writeDecimal((BigDecimal)val);

                    break;

                case STRING:
                    writer.writeString((String)val);

                    break;

                case UUID:
                    writer.writeUuid((UUID)val);

                    break;

                case DATE:
                    writer.writeDate((Date)val);

                    break;

                case TIMESTAMP:
                    writer.writeTimestamp((Timestamp)val);

                    break;

                case TIME:
                    writer.writeTime((Time)val);

                    break;

                case BYTE_ARR:
                    writer.writeByteArray((byte[])val);

                    break;

                case SHORT_ARR:
                    writer.writeShortArray((short[])val);

                    break;

                case INT_ARR:
                    writer.writeIntArray((int[])val);

                    break;

                case LONG_ARR:
                    writer.writeLongArray((long[])val);

                    break;

                case FLOAT_ARR:
                    writer.writeFloatArray((float[])val);

                    break;

                case DOUBLE_ARR:
                    writer.writeDoubleArray((double[])val);

                    break;

                case CHAR_ARR:
                    writer.writeCharArray((char[])val);

                    break;

                case BOOLEAN_ARR:
                    writer.writeBooleanArray((boolean[])val);

                    break;

                case DECIMAL_ARR:
                    writer.writeDecimalArray((BigDecimal[])val);

                    break;

                case STRING_ARR:
                    writer.writeStringArray((String[])val);

                    break;

                case UUID_ARR:
                    writer.writeUuidArray((UUID[])val);

                    break;

                case DATE_ARR:
                    writer.writeDateArray((Date[])val);

                    break;

                case TIMESTAMP_ARR:
                    writer.writeTimestampArray((Timestamp[])val);

                    break;

                case TIME_ARR:
                    writer.writeTimeArray((Time[])val);

                    break;

                case OBJECT_ARR:
                    writer.writeObjectArray((Object[])val);

                    break;

                case COL:
                    writer.writeCollection((Collection<?>)val);

                    break;

                case MAP:
                    writer.writeMap((Map<?, ?>)val);

                    break;

                case BINARY_OBJ:
                    writer.writeBinaryObject((BinaryObjectImpl)val);

                    break;

                case ENUM:
                    writer.writeEnum((Enum<?>)val);

                    break;

                case BINARY_ENUM:
                    writer.writeBinaryEnum((BinaryEnumObjectImpl)val);

                    break;

                case ENUM_ARR:
                    writer.doWriteEnumArray((Object[])val);

                    break;

                case BINARY:
                case OBJECT:
                case PROXY:
                    writer.writeObject(val);

                    break;

                case CLASS:
                    writer.writeClass((Class)val);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }
        }

        /**
         * @param val Val to get write mode for.
         * @return Write mode.
         */
        protected BinaryWriteMode mode(Object val) {
            return dynamic ?
                val == null ? BinaryWriteMode.OBJECT : BinaryUtils.mode(val.getClass()) :
                mode;
        }
    }
}
