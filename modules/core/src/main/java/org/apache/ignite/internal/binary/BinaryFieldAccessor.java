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
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Field accessor to speedup access.
 */
public abstract class BinaryFieldAccessor {
    /** Field ID. */
    protected final int id;

    /** Field name */
    protected final String name;

    /** Mode. */
    protected final BinaryWriteMode mode;

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
                return new DefaultFinalClassAccessor(field, id, mode, !U.isFinal(field.getType()));
        }
    }

    /**
     * Protected constructor.
     *
     * @param id Field ID.
     * @param mode Mode;
     */
    protected BinaryFieldAccessor(Field field, int id, BinaryWriteMode mode) {
        assert field != null;
        assert id != 0;
        assert mode != null;

        this.name = field.getName();
        this.id = id;
        this.mode = mode;
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
        catch (Exception ex) {
            if (ex instanceof UnregisteredClassException)
                throw ex;

            if (S.INCLUDE_SENSITIVE && !F.isEmpty(name))
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
     * Read field.
     *
     * @param obj Object.
     * @param reader Reader.
     * @throws BinaryObjectException If failed.
     */
    public void read(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
        try {
            read0(obj, reader);
        }
        catch (Exception ex) {
            if (S.INCLUDE_SENSITIVE && !F.isEmpty(name))
                throw new BinaryObjectException("Failed to read field [name=" + name + ']', ex);
            else
                throw new BinaryObjectException("Failed to read field [id=" + id + ']', ex);
        }
    }

    /**
     * Read field.
     *
     * @param obj Object.
     * @param reader Reader.
     * @throws BinaryObjectException If failed.
     */
    protected abstract void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException;

    /**
     * Base primitive field accessor.
     */
    private static abstract class AbstractPrimitiveAccessor extends BinaryFieldAccessor {
        /** Offset. */
        protected final long offset;

        /**
         * Constructor.
         *
         * @param field Field.
         * @param id Field ID.
         * @param mode Mode.
         */
        protected AbstractPrimitiveAccessor(Field field, int id, BinaryWriteMode mode) {
            super(field, id, mode);

            offset = GridUnsafe.objectFieldOffset(field);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            byte val = reader.readByte(id);

            GridUnsafe.putByteField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            boolean val = reader.readBoolean(id);

            GridUnsafe.putBooleanField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            short val = reader.readShort(id);

            GridUnsafe.putShortField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            char val = reader.readChar(id);

            GridUnsafe.putCharField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            int val = reader.readInt(id);

            GridUnsafe.putIntField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            long val = reader.readLong(id);

            GridUnsafe.putLongField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            float val = reader.readFloat(id);

            GridUnsafe.putFloatField(obj, offset, val);
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

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            double val = reader.readDouble(id);

            GridUnsafe.putDoubleField(obj, offset, val);
        }
    }

    /**
     * Default accessor.
     */
    private static class DefaultFinalClassAccessor extends BinaryFieldAccessor {
        /** Target field. */
        private final Field field;

        /** Dynamic accessor flag. */
        private final boolean dynamic;

        /**
         * Constructor.
         *
         * @param field Field.
         * @param id Field ID.
         * @param mode Mode.
         */
        DefaultFinalClassAccessor(Field field, int id, BinaryWriteMode mode, boolean dynamic) {
            super(field, id, mode);

            this.field = field;
            this.dynamic = dynamic;
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
                    writer.writeDecimalField((BigDecimal)val);

                    break;

                case STRING:
                    writer.writeStringField((String)val);

                    break;

                case UUID:
                    writer.writeUuidField((UUID)val);

                    break;

                case DATE:
                    writer.writeDateField((Date)val);

                    break;

                case TIMESTAMP:
                    writer.writeTimestampField((Timestamp)val);

                    break;

                case TIME:
                    writer.writeTimeField((Time)val);

                    break;

                case BYTE_ARR:
                    writer.writeByteArrayField((byte[])val);

                    break;

                case SHORT_ARR:
                    writer.writeShortArrayField((short[])val);

                    break;

                case INT_ARR:
                    writer.writeIntArrayField((int[])val);

                    break;

                case LONG_ARR:
                    writer.writeLongArrayField((long[])val);

                    break;

                case FLOAT_ARR:
                    writer.writeFloatArrayField((float[])val);

                    break;

                case DOUBLE_ARR:
                    writer.writeDoubleArrayField((double[])val);

                    break;

                case CHAR_ARR:
                    writer.writeCharArrayField((char[])val);

                    break;

                case BOOLEAN_ARR:
                    writer.writeBooleanArrayField((boolean[])val);

                    break;

                case DECIMAL_ARR:
                    writer.writeDecimalArrayField((BigDecimal[])val);

                    break;

                case STRING_ARR:
                    writer.writeStringArrayField((String[])val);

                    break;

                case UUID_ARR:
                    writer.writeUuidArrayField((UUID[])val);

                    break;

                case DATE_ARR:
                    writer.writeDateArrayField((Date[])val);

                    break;

                case TIMESTAMP_ARR:
                    writer.writeTimestampArrayField((Timestamp[])val);

                    break;

                case TIME_ARR:
                    writer.writeTimeArrayField((Time[])val);

                    break;

                case OBJECT_ARR:
                    writer.writeObjectArrayField((Object[])val);

                    break;

                case COL:
                    writer.writeCollectionField((Collection<?>)val);

                    break;

                case MAP:
                    writer.writeMapField((Map<?, ?>)val);

                    break;

                case BINARY_OBJ:
                    writer.writeBinaryObjectField((BinaryObjectImpl)val);

                    break;

                case ENUM:
                    writer.writeEnumField((Enum<?>)val);

                    break;

                case BINARY_ENUM:
                    writer.doWriteBinaryEnum((BinaryEnumObjectImpl)val);

                    break;

                case ENUM_ARR:
                    writer.writeEnumArrayField((Object[])val);

                    break;

                case BINARY:
                case OBJECT:
                case PROXY:
                    writer.writeObjectField(val);

                    break;

                case CLASS:
                    writer.writeClassField((Class)val);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }
        }

        /** {@inheritDoc} */
        @Override public void read0(Object obj, BinaryReaderExImpl reader) throws BinaryObjectException {
            Object val = dynamic ? reader.readField(id) : readFixedType(reader);

            try {
                if (val != null || !field.getType().isPrimitive())
                    field.set(obj, val);
            }
            catch (IllegalAccessException e) {
                throw new BinaryObjectException("Failed to set value for field: " + field, e);
            }
        }

        /**
         * Reads fixed type from the given reader with flags validation.
         *
         * @param reader Reader to read from.
         * @return Read value.
         * @throws BinaryObjectException If failed to read value from the stream.
         */
        protected Object readFixedType(BinaryReaderExImpl reader) throws BinaryObjectException {
            Object val = null;

            switch (mode) {
                case BYTE:
                    val = reader.readByteNullable(id);

                    break;

                case SHORT:
                    val = reader.readShortNullable(id);

                    break;

                case INT:
                    val = reader.readIntNullable(id);

                    break;

                case LONG:
                    val = reader.readLongNullable(id);

                    break;

                case FLOAT:
                    val = reader.readFloatNullable(id);

                    break;

                case DOUBLE:
                    val = reader.readDoubleNullable(id);

                    break;

                case CHAR:
                    val = reader.readCharNullable(id);

                    break;

                case BOOLEAN:
                    val = reader.readBooleanNullable(id);

                    break;

                case DECIMAL:
                    val = reader.readDecimal(id);

                    break;

                case STRING:
                    val = reader.readString(id);

                    break;

                case UUID:
                    val = reader.readUuid(id);

                    break;

                case DATE:
                    val = reader.readDate(id);

                    break;

                case TIMESTAMP:
                    val = reader.readTimestamp(id);

                    break;

                case TIME:
                    val = reader.readTime(id);

                    break;

                case BYTE_ARR:
                    val = reader.readByteArray(id);

                    break;

                case SHORT_ARR:
                    val = reader.readShortArray(id);

                    break;

                case INT_ARR:
                    val = reader.readIntArray(id);

                    break;

                case LONG_ARR:
                    val = reader.readLongArray(id);

                    break;

                case FLOAT_ARR:
                    val = reader.readFloatArray(id);

                    break;

                case DOUBLE_ARR:
                    val = reader.readDoubleArray(id);

                    break;

                case CHAR_ARR:
                    val = reader.readCharArray(id);

                    break;

                case BOOLEAN_ARR:
                    val = reader.readBooleanArray(id);

                    break;

                case DECIMAL_ARR:
                    val = reader.readDecimalArray(id);

                    break;

                case STRING_ARR:
                    val = reader.readStringArray(id);

                    break;

                case UUID_ARR:
                    val = reader.readUuidArray(id);

                    break;

                case DATE_ARR:
                    val = reader.readDateArray(id);

                    break;

                case TIMESTAMP_ARR:
                    val = reader.readTimestampArray(id);

                    break;

                case TIME_ARR:
                    val = reader.readTimeArray(id);

                    break;

                case OBJECT_ARR:
                    val = reader.readObjectArray(id);

                    break;

                case COL:
                    val = reader.readCollection(id, null);

                    break;

                case MAP:
                    val = reader.readMap(id, null);

                    break;

                case BINARY_OBJ:
                    val = reader.readBinaryObject(id);

                    break;

                case ENUM:
                    val = reader.readEnum(id, field.getType());

                    break;

                case ENUM_ARR:
                    val = reader.readEnumArray(id, field.getType().getComponentType());

                    break;

                case BINARY_ENUM:
                    val = reader.readBinaryEnum(id);

                    break;

                case BINARY:
                case OBJECT:
                    val = reader.readObject(id);

                    break;

                case CLASS:
                    val = reader.readClass(id);

                    break;

                default:
                    assert false : "Invalid mode: " + mode;
            }

            return val;
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
