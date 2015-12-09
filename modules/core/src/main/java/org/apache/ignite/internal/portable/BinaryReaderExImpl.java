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

package org.apache.ignite.internal.portable;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLASS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DFLT_HDR_LEN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HANDLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OPTM_MARSH;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PORTABLE_OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID_ARR;

/**
 * Portable reader implementation.
 */
@SuppressWarnings("unchecked")
public class BinaryReaderExImpl implements BinaryReader, BinaryRawReaderEx, BinaryReaderHandlesHolder, ObjectInput {
    /** Portable context. */
    private final PortableContext ctx;

    /** Input stream. */
    private final PortableInputStream in;

    /** Class loaded. */
    private final ClassLoader ldr;

    /** Reader context which is constantly passed between objects. */
    private BinaryReaderHandles hnds;

    /** */
    private final int start;

    /** Start of actual data. Positioned right after the header. */
    private final int dataStart;

    /** Type ID. */
    private final int typeId;

    /** Raw offset. */
    private final int rawOff;

    /** Footer start. */
    private final int footerStart;

    /** Footer end. */
    private final int footerLen;

    /** ID mapper. */
    private final BinaryIdMapper idMapper;

    /** Schema Id. */
    private final int schemaId;

    /** Whether this is user type or not. */
    private final boolean userType;

    /** Whether field IDs exist. */
    private final int fieldIdLen;

    /** Offset size in bytes. */
    private final int fieldOffsetLen;

    /** Object schema. */
    private final PortableSchema schema;

    /** Whether passed IDs matches schema order. Reset to false as soon as a single mismatch detected. */
    private boolean matching = true;

    /** Order of a field whose match is expected. */
    private int matchingOrder;

    /** Whether stream is in raw mode. */
    private boolean raw;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     */
    public BinaryReaderExImpl(PortableContext ctx, PortableInputStream in, ClassLoader ldr) {
        this(ctx, in, ldr, null);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     */
    public BinaryReaderExImpl(PortableContext ctx, PortableInputStream in, ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds) {
        this(ctx, in, ldr, hnds, false);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param skipHdrCheck Whether to skip header check.
     */
    public BinaryReaderExImpl(PortableContext ctx, PortableInputStream in, ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds, boolean skipHdrCheck) {
        // Initialize base members.
        this.ctx = ctx;
        this.in = in;
        this.ldr = ldr;
        this.hnds = hnds;

        start = in.position();

        // Perform full header parsing in case of portable object.
        if (!skipHdrCheck && (in.readByte() == GridPortableMarshaller.OBJ)) {
            // Ensure protocol is fine.
            PortableUtils.checkProtocolVersion(in.readByte());

            // Read header content.
            short flags = in.readShort();
            int typeId0 = in.readInt();

            in.readInt(); // Skip hash code.

            int len = in.readInt();
            schemaId = in.readInt();
            int offset = in.readInt();

            // Get trivial flag values.
            userType = PortableUtils.isUserType(flags);
            fieldIdLen = PortableUtils.fieldIdLength(flags);
            fieldOffsetLen = PortableUtils.fieldOffsetLength(flags);

            // Calculate footer borders and raw offset.
            if (PortableUtils.hasSchema(flags)) {
                // Schema exists.
                footerStart = start + offset;

                if (PortableUtils.hasRaw(flags)) {
                    footerLen = len - offset - 4;
                    rawOff = start + in.readIntPositioned(start + len - 4);
                }
                else {
                    footerLen = len - offset;
                    rawOff = start + len;
                }
            }
            else {
                // No schema.
                footerStart = start + len;
                footerLen = 0;

                if (PortableUtils.hasRaw(flags))
                    rawOff = start + offset;
                else
                    rawOff = start + len;
            }

            // Finally, we have to resolve real type ID.
            if (typeId0 == UNREGISTERED_TYPE_ID) {
                int off = in.position();

                // Registers class by type ID, at least locally if the cache is not ready yet.
                typeId = ctx.descriptorForClass(PortableUtils.doReadClass(in, ctx, ldr, typeId0), false).typeId();

                int clsNameLen = in.position() - off;

                dataStart = start + DFLT_HDR_LEN + clsNameLen;
            }
            else {
                typeId = typeId0;

                dataStart = start + DFLT_HDR_LEN;
            }

            idMapper = userType ? ctx.userTypeIdMapper(typeId) : BinaryInternalIdMapper.defaultInstance();
            schema = PortableUtils.hasSchema(flags) ? getOrCreateSchema() : null;
        }
        else {
            dataStart = 0;
            typeId = 0;
            rawOff = 0;
            footerStart = 0;
            footerLen = 0;
            idMapper = null;
            schemaId = 0;
            userType = false;
            fieldIdLen = 0;
            fieldOffsetLen = 0;
            schema = null;
        }

        streamPosition(start);
    }

    /**
     * @return Input stream.
     */
    public PortableInputStream in() {
        return in;
    }

    /**
     * @return Descriptor.
     */
    PortableClassDescriptor descriptor() {
        return ctx.descriptorForTypeId(userType, typeId, ldr, true);
    }

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshal(int offset) throws BinaryObjectException {
        streamPosition(offset);

        return in.position() >= 0 ? PortableUtils.unmarshal(in, ctx, ldr, this) : null;
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? PortableUtils.unmarshal(in, ctx, ldr, this) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? PortableUtils.unmarshal(in, ctx, ldr, this) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Portable object.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable BinaryObject readPortableObject(int fieldId) throws BinaryObjectException {
        if (findFieldById(fieldId)) {
            if (checkFlag(PORTABLE_OBJ) == Flag.NULL)
                return null;

            return new BinaryObjectImpl(ctx, PortableUtils.doReadByteArray(in), in.readInt());
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Field class.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Class<?> readClass(int fieldId) throws BinaryObjectException {
        if (findFieldById(fieldId)) {
            if (checkFlag(CLASS) == Flag.NULL)
                return null;

            return PortableUtils.doReadClass(in, ctx, ldr);
        }

        return null;
    }

    /**
     * @param obj Object.
     */
    void setHandle(Object obj) {
        setHandle(obj, start);
    }

    /** {@inheritDoc} */
    @Override public void setHandle(Object obj, int pos) {
        handles().put(pos, obj);
    }

    /** {@inheritDoc} */
    @Override public Object getHandle(int pos) {
        return hnds != null ? hnds.get(pos) : null;
    }

    /** {@inheritDoc} */
    @Override public BinaryReaderHandles handles() {
        if (hnds == null)
            hnds = new BinaryReaderHandles();

        return hnds;
    }

    /**
     * Recreating field value from a handle.
     *
     * @param <T> Field type.
     * @return Field.
     */
    private <T> T readHandleField() {
        int handlePos = PortableUtils.positionForHandle(in) - in.readInt();

        Object obj = getHandle(handlePos);

        if (obj == null) {
            int retPos = in.position();

            streamPosition(handlePos);

            obj = PortableUtils.doReadObject(in, ctx, ldr, this);

            streamPosition(retPos);
        }

        return (T)obj;
    }
    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(BYTE) == Flag.NORMAL ? in.readByte() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    byte readByte(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(BYTE) == Flag.NORMAL ? in.readByte() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Byte readByteNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(BYTE) == Flag.NORMAL ? in.readByte() : null;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws BinaryObjectException {
        return in.readByte();
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readByteArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable byte[] readByteArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readByteArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws BinaryObjectException {
        switch (checkFlag(BYTE_ARR)) {
            case NORMAL:
                return PortableUtils.doReadByteArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(BOOLEAN) == Flag.NORMAL && in.readBoolean();
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    boolean readBoolean(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(BOOLEAN) == Flag.NORMAL && in.readBoolean();
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Boolean readBooleanNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(BOOLEAN) == Flag.NORMAL ? in.readBoolean() : null;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws BinaryObjectException {
        return in.readBoolean();
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readBooleanArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable boolean[] readBooleanArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readBooleanArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws BinaryObjectException {
        switch (checkFlag(BOOLEAN_ARR)) {
            case NORMAL:
                return PortableUtils.doReadBooleanArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(SHORT) == Flag.NORMAL ? in.readShort() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    short readShort(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(SHORT) == Flag.NORMAL ? in.readShort() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Short readShortNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(SHORT) == Flag.NORMAL ? in.readShort() : null;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws BinaryObjectException {
        return in.readShort();
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readShortArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable short[] readShortArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readShortArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws BinaryObjectException {
        switch (checkFlag(SHORT_ARR)) {
            case NORMAL:
                return PortableUtils.doReadShortArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(CHAR) == Flag.NORMAL ? in.readChar() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    char readChar(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(CHAR) == Flag.NORMAL ? in.readChar() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Character readCharNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(CHAR) == Flag.NORMAL ? in.readChar() : null;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws BinaryObjectException {
        return in.readChar();
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readCharArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable char[] readCharArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readCharArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws BinaryObjectException {
        switch (checkFlag(CHAR_ARR)) {
            case NORMAL:
                return PortableUtils.doReadCharArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(INT) == Flag.NORMAL ? in.readInt() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    int readInt(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(INT) == Flag.NORMAL ? in.readInt() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Integer readIntNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(INT) == Flag.NORMAL ? in.readInt() : null;
    }
    
    /** {@inheritDoc} */
    @Override public int readInt() throws BinaryObjectException {
        return in.readInt();
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readIntArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable int[] readIntArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readIntArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws BinaryObjectException {
        switch (checkFlag(INT_ARR)) {
            case NORMAL:
                return PortableUtils.doReadIntArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(LONG) == Flag.NORMAL ? in.readLong() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    long readLong(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(LONG) == Flag.NORMAL ? in.readLong() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Long readLongNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(LONG) == Flag.NORMAL ? in.readLong() : null;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws BinaryObjectException {
        return in.readLong();
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readLongArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable long[] readLongArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readLongArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws BinaryObjectException {
        switch (checkFlag(LONG_ARR)) {
            case NORMAL:
                return PortableUtils.doReadLongArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(FLOAT) == Flag.NORMAL ? in.readFloat() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    float readFloat(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(FLOAT) == Flag.NORMAL ? in.readFloat() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Float readFloatNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(FLOAT) == Flag.NORMAL ? in.readFloat() : null;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws BinaryObjectException {
        return in.readFloat();
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readFloatArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable float[] readFloatArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readFloatArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws BinaryObjectException {
        switch (checkFlag(FLOAT_ARR)) {
            case NORMAL:
                return PortableUtils.doReadFloatArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) && checkFlagNoHandles(DOUBLE) == Flag.NORMAL ? in.readDouble() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    double readDouble(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(DOUBLE) == Flag.NORMAL ? in.readDouble() : 0;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Double readDoubleNullable(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) && checkFlagNoHandles(DOUBLE) == Flag.NORMAL ? in.readDouble() : null;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws BinaryObjectException {
        return in.readDouble();
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readDoubleArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable double[] readDoubleArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readDoubleArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws BinaryObjectException {
        switch (checkFlag(DOUBLE_ARR)) {
            case NORMAL:
                return PortableUtils.doReadDoubleArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readDecimal() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable BigDecimal readDecimal(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readDecimal() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal() throws BinaryObjectException {
        return checkFlagNoHandles(DECIMAL) == Flag.NORMAL ? PortableUtils.doReadDecimal(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readDecimalArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable BigDecimal[] readDecimalArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readDecimalArray() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray() throws BinaryObjectException {
        switch (checkFlag(DECIMAL_ARR)) {
            case NORMAL:
                return PortableUtils.doReadDecimalArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public String readString(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readString() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable String readString(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readString() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String readString() throws BinaryObjectException {
        return checkFlagNoHandles(STRING) == Flag.NORMAL ? PortableUtils.doReadString(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String[] readStringArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readStringArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable String[] readStringArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readStringArray() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String[] readStringArray() throws BinaryObjectException {
        switch (checkFlag(STRING_ARR)) {
            case NORMAL:
                return PortableUtils.doReadStringArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public UUID readUuid(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readUuid() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable UUID readUuid(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readUuid() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public UUID readUuid() throws BinaryObjectException {
        return checkFlagNoHandles(UUID) == Flag.NORMAL ? PortableUtils.doReadUuid(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public UUID[] readUuidArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readUuidArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable UUID[] readUuidArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readUuidArray() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public UUID[] readUuidArray() throws BinaryObjectException {
        switch (checkFlag(UUID_ARR)) {
            case NORMAL:
                return PortableUtils.doReadUuidArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Date readDate(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readDate() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Date readDate(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readDate() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Date readDate() throws BinaryObjectException {
        return checkFlagNoHandles(DATE) == Flag.NORMAL ? PortableUtils.doReadDate(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Date[] readDateArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readDateArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Date[] readDateArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readDateArray() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Date[] readDateArray() throws BinaryObjectException {
        switch (checkFlag(DATE_ARR)) {
            case NORMAL:
                return PortableUtils.doReadDateArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Timestamp readTimestamp(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readTimestamp() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Timestamp readTimestamp(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readTimestamp() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Timestamp readTimestamp() throws BinaryObjectException {
        return checkFlagNoHandles(TIMESTAMP) == Flag.NORMAL ? PortableUtils.doReadTimestamp(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Timestamp[] readTimestampArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readTimestampArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Timestamp[] readTimestampArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readTimestampArray() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Timestamp[] readTimestampArray() throws BinaryObjectException {
        switch (checkFlag(TIMESTAMP_ARR)) {
            case NORMAL:
                return PortableUtils.doReadTimestampArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T readObject(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? (T)PortableUtils.doReadObject(in, ctx, ldr, this) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object readObject(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? PortableUtils.doReadObject(in, ctx, ldr, this) : null;
    }

    /** {@inheritDoc} */
    @Override public Object readObject() throws BinaryObjectException {
        return PortableUtils.doReadObject(in, ctx, ldr, this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObjectDetached() throws BinaryObjectException {
        return PortableUtils.unmarshal(in, ctx, ldr, this, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readObjectArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object[] readObjectArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readObjectArray() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws BinaryObjectException {
        switch (checkFlag(OBJ_ARR)) {
            case NORMAL:
                return PortableUtils.doReadObjectArray(in, ctx, ldr, this, true);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? (T)readEnum0(null) : null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Enum<?> readEnum(int fieldId, @Nullable Class<?> cls) throws BinaryObjectException {
        return findFieldById(fieldId) ? readEnum0(cls) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum() throws BinaryObjectException {
        return (T)readEnum0(null);
    }

    /**
     * Internal routine to read enum for named field.
     *
     * @param cls Class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private Enum<?> readEnum0(@Nullable Class<?> cls) throws BinaryObjectException {
        if (checkFlagNoHandles(ENUM) == Flag.NORMAL) {
            // Read class even if we know it in advance to set correct stream position.
            Class<?> cls0 = PortableUtils.doReadClass(in, ctx, ldr);

            if (cls == null)
                cls = cls0;

            return PortableUtils.doReadEnum(in, cls);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray(String fieldName)
        throws BinaryObjectException {
        return findFieldByName(fieldName) ? (T[])readEnumArray0(null) : null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object[] readEnumArray(int fieldId, @Nullable Class<?> cls) throws BinaryObjectException {
        return findFieldById(fieldId) ? readEnumArray0(cls) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray() throws BinaryObjectException {
        return (T[])readEnumArray0(null);
    }

    /**
     * Internal routine to read enum for named field.
     *
     * @param cls Class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private Object[] readEnumArray0(@Nullable Class<?> cls) throws BinaryObjectException {
        switch (checkFlag(ENUM_ARR)) {
            case NORMAL:
                // Read class even if we know it in advance to set correct stream position.
                Class<?> cls0 = PortableUtils.doReadClass(in, ctx, ldr);

                if (cls == null)
                    cls = cls0;

                return PortableUtils.doReadEnumArray(in, ctx, ldr, cls);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? (Collection<T>)readCollection0(null) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName, BinaryCollectionFactory<T> factory)
        throws BinaryObjectException {
        return findFieldByName(fieldName) ? readCollection0(factory) : null;
    }

    /**
     * @param fieldId Field ID.
     * @param factory Collection factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable <T> Collection<T> readCollection(int fieldId, @Nullable BinaryCollectionFactory<T> factory)
        throws BinaryObjectException {
        return findFieldById(fieldId) ? (Collection<T>)readCollection0(factory) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws BinaryObjectException {
        return readCollection0(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(BinaryCollectionFactory<T> factory)
        throws BinaryObjectException {
        return readCollection0(factory);
    }

    /**
     * Internal read collection routine.
     *
     * @param factory Collection factory.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    private Collection readCollection0(@Nullable BinaryCollectionFactory factory)
        throws BinaryObjectException {
        switch (checkFlag(COL)) {
            case NORMAL:
                return (Collection)PortableUtils.doReadCollection(in, ctx, ldr, this, true, factory);

            case HANDLE: {
                int handlePos = PortableUtils.positionForHandle(in) - in.readInt();

                Object obj = getHandle(handlePos);

                if (obj == null) {
                    int retPos = in.position();

                    streamPosition(handlePos);

                    obj = readCollection0(factory);

                    streamPosition(retPos);
                }

                return (Collection)obj;
            }

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? (Map<K, V>)readMap0(null) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName, BinaryMapFactory<K, V> factory)
        throws BinaryObjectException {
        return findFieldByName(fieldName) ? readMap0(factory) : null;
    }

    /**
     * @param fieldId Field ID.
     * @param factory Factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Map<?, ?> readMap(int fieldId, @Nullable BinaryMapFactory factory) throws BinaryObjectException {
        return findFieldById(fieldId) ? readMap0(factory) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws BinaryObjectException {
        return readMap0(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(BinaryMapFactory<K, V> factory)
        throws BinaryObjectException {
        return readMap0(factory);
    }

    /**
     * Internal read map routine.
     *
     * @param factory Factory.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    private Map readMap0(@Nullable BinaryMapFactory factory) throws BinaryObjectException {
        switch (checkFlag(MAP)) {
            case NORMAL:
                return (Map)PortableUtils.doReadMap(in, ctx, ldr, this, true, factory);

            case HANDLE: {
                int handlePos = PortableUtils.positionForHandle(in) - in.readInt();

                Object obj = getHandle(handlePos);

                if (obj == null) {
                    int retPos = in.position();

                    streamPosition(handlePos);

                    obj = readMap0(factory);

                    streamPosition(retPos);
                }

                return (Map)obj;
            }

            default:
                return null;
        }
    }

    /**
     * Ensure that type flag is either null, handle or contains expected value.
     *
     * @param expFlag Expected value.
     * @return Flag mode.
     * @throws BinaryObjectException If flag is neither null, nor handle or expected.
     */
    private Flag checkFlag(byte expFlag) {
        byte flag = in.readByte();

        if (flag == expFlag)
            return Flag.NORMAL;
        else if (flag == NULL)
            return Flag.NULL;
        else if (flag == HANDLE)
            return Flag.HANDLE;

        int pos = PortableUtils.positionForHandle(in);

        throw new BinaryObjectException("Unexpected flag value [pos=" + pos + ", expected=" + expFlag +
            ", actual=" + flag + ']');
    }

    /**
     * Ensure that type flag is either null or contains expected value.
     *
     * @param expFlag Expected value.
     * @return Flag mode.
     * @throws BinaryObjectException If flag is neither null, nor expected.
     */
    private Flag checkFlagNoHandles(byte expFlag) {
        byte flag = in.readByte();

        if (flag == expFlag)
            return Flag.NORMAL;
        else if (flag == NULL)
            return Flag.NULL;

        int pos = PortableUtils.positionForHandle(in);

        throw new BinaryObjectException("Unexpected flag value [pos=" + pos + ", expected=" + expFlag +
            ", actual=" + flag + ']');
    }

    /** {@inheritDoc} */
    @Override public BinaryRawReader rawReader() {
        if (!raw) {
            streamPositionRandom(rawOff);

            raw = true;

            return this;
        }
        else
            throw new BinaryObjectException("Method \"rawReader\" can be called only once.");
    }

    /**
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    @Nullable Object deserialize() throws BinaryObjectException {
        Object obj;

        byte flag = in.readByte();

        switch (flag) {
            case NULL:
                obj = null;

                break;

            case HANDLE:
                int handlePos = start - in.readInt();

                obj = getHandle(handlePos);

                if (obj == null) {
                    int retPos = in.position();

                    streamPosition(handlePos);

                    obj = PortableUtils.doReadObject(in, ctx, ldr, this);

                    streamPosition(retPos);
                }

                break;

            case OBJ:
                PortableClassDescriptor desc = ctx.descriptorForTypeId(userType, typeId, ldr, true);

                streamPosition(dataStart);

                if (desc == null)
                    throw new BinaryInvalidTypeException("Unknown type ID: " + typeId);

                obj = desc.read(this);

                streamPosition(footerStart + footerLen);

                break;

            case BYTE:
                obj = in.readByte();

                break;

            case SHORT:
                obj = in.readShort();

                break;

            case INT:
                obj = in.readInt();

                break;

            case LONG:
                obj = in.readLong();

                break;

            case FLOAT:
                obj = in.readFloat();

                break;

            case DOUBLE:
                obj = in.readDouble();

                break;

            case CHAR:
                obj = in.readChar();

                break;

            case BOOLEAN:
                obj = in.readBoolean();

                break;

            case DECIMAL:
                obj = PortableUtils.doReadDecimal(in);

                break;

            case STRING:
                obj = PortableUtils.doReadString(in);

                break;

            case UUID:
                obj = PortableUtils.doReadUuid(in);

                break;

            case DATE:
                obj = PortableUtils.doReadDate(in);

                break;

            case TIMESTAMP:
                obj = PortableUtils.doReadTimestamp(in);

                break;

            case BYTE_ARR:
                obj = PortableUtils.doReadByteArray(in);

                break;

            case SHORT_ARR:
                obj = PortableUtils.doReadShortArray(in);

                break;

            case INT_ARR:
                obj = PortableUtils.doReadIntArray(in);

                break;

            case LONG_ARR:
                obj = PortableUtils.doReadLongArray(in);

                break;

            case FLOAT_ARR:
                obj = PortableUtils.doReadFloatArray(in);

                break;

            case DOUBLE_ARR:
                obj = PortableUtils.doReadDoubleArray(in);

                break;

            case CHAR_ARR:
                obj = PortableUtils.doReadCharArray(in);

                break;

            case BOOLEAN_ARR:
                obj = PortableUtils.doReadBooleanArray(in);

                break;

            case DECIMAL_ARR:
                obj = PortableUtils.doReadDecimalArray(in);

                break;

            case STRING_ARR:
                obj = PortableUtils.doReadStringArray(in);

                break;

            case UUID_ARR:
                obj = PortableUtils.doReadUuidArray(in);

                break;

            case DATE_ARR:
                obj = PortableUtils.doReadDateArray(in);

                break;

            case TIMESTAMP_ARR:
                obj = PortableUtils.doReadTimestampArray(in);

                break;

            case OBJ_ARR:
                obj = PortableUtils.doReadObjectArray(in, ctx, ldr, this, true);

                break;

            case COL:
                obj = PortableUtils.doReadCollection(in, ctx, ldr, this, true, null);

                break;

            case MAP:
                obj = PortableUtils.doReadMap(in, ctx, ldr, this, true, null);

                break;

            case PORTABLE_OBJ:
                obj = PortableUtils.doReadPortableObject(in, ctx);

                ((BinaryObjectImpl)obj).context(ctx);

                if (!GridPortableMarshaller.KEEP_PORTABLES.get())
                    obj = ((BinaryObject)obj).deserialize();

                break;

            case ENUM:
                obj = PortableUtils.doReadEnum(in, PortableUtils.doReadClass(in, ctx, ldr));

                break;

            case ENUM_ARR:
                obj = PortableUtils.doReadEnumArray(in, ctx, ldr, PortableUtils.doReadClass(in, ctx, ldr));

                break;

            case CLASS:
                obj = PortableUtils.doReadClass(in, ctx, ldr);

                break;

            case OPTM_MARSH:
                obj = PortableUtils.doReadOptimized(in, ctx, ldr);

                break;

            default:
                throw new BinaryObjectException("Invalid flag value: " + flag);
        }

        return obj;
    }

    /**
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    @Nullable Object readField(int fieldId) throws BinaryObjectException {
        if (!findFieldById(fieldId))
            return null;

        return new BinaryReaderExImpl(ctx, in, ldr, hnds).deserialize();
    }

    /**
     * @param name Field name.
     * @return Field offset.
     */
    private int fieldId(String name) {
        assert name != null;

        return idMapper.fieldId(typeId, name);
    }

    /**
     * Get or create object schema.
     *
     * @return Schema.
     */
    public PortableSchema getOrCreateSchema() {
        PortableSchema schema = ctx.schemaRegistry(typeId).schema(schemaId);

        if (schema == null) {
            if (fieldIdLen != PortableUtils.FIELD_ID_LEN) {
                BinaryTypeImpl type = (BinaryTypeImpl)ctx.metadata(typeId);

                if (type == null || type.metadata() == null)
                    throw new BinaryObjectException("Cannot find metadata for object with compact footer: " +
                        typeId);

                for (PortableSchema typeSchema : type.metadata().schemas()) {
                    if (schemaId == typeSchema.schemaId()) {
                        schema = typeSchema;

                        break;
                    }
                }

                if (schema == null)
                    throw new BinaryObjectException("Cannot find schema for object with compact footer [" +
                        "typeId=" + typeId + ", schemaId=" + schemaId + ']');
            }
            else
                schema = createSchema();

            assert schema != null;

            ctx.schemaRegistry(typeId).addSchema(schemaId, schema);
        }

        return schema;
    }

    /**
     * Create schema.
     *
     * @return Schema.
     */
    private PortableSchema createSchema() {
        assert fieldIdLen == PortableUtils.FIELD_ID_LEN;

        PortableSchema.Builder builder = PortableSchema.Builder.newBuilder();

        int searchPos = footerStart;
        int searchEnd = searchPos + footerLen;

        while (searchPos < searchEnd) {
            int fieldId = in.readIntPositioned(searchPos);

            builder.addField(fieldId);

            searchPos += PortableUtils.FIELD_ID_LEN + fieldOffsetLen;
        }

        return builder.build();
    }

    /**
     * Try finding the field by name.
     *
     * @param name Field name.
     * @return Offset.
     */
    public boolean findFieldByName(String name) {
        if (raw)
            throw new BinaryObjectException("Failed to read named field because reader is in raw mode.");

        assert dataStart != start;

        if (footerLen == 0)
            return false;

        if (userType) {
            int order;

            if (matching) {
                int expOrder = matchingOrder++;

                PortableSchema.Confirmation confirm = schema.confirmOrder(expOrder, name);

                switch (confirm) {
                    case CONFIRMED:
                        // The best case: got order without ID calculation and (ID -> order) lookup.
                        if (expOrder == 0)
                            // When we read the very first field, position is set to start, hence this re-positioning.
                            streamPosition(dataStart);

                        return true;

                    case REJECTED:
                        // Rejected, no more speculations are possible. Fallback to the slowest scenario.
                        matching = false;

                        order = schema.order(fieldId(name));

                        break;

                    default:
                        // Field name is not know for this order. Need to calculate ID and repeat speculation.
                        assert confirm == PortableSchema.Confirmation.CLARIFY;

                        int id = fieldId(name);
                        int realId = schema.fieldId(expOrder);

                        if (id == realId) {
                            // IDs matched, cache field name inside schema.
                            schema.clarifyFieldName(expOrder, name);

                            if (expOrder == 0)
                                streamPosition(dataStart);

                            return true;
                        }
                        else {
                            // No match, stop further speculations.
                            matching = false;

                            order = schema.order(id);
                        }

                        break;
                }
            }
            else
                order = schema.order(fieldId(name));

            return trySetUserFieldPosition(order);
        }
        else
            return trySetSystemFieldPosition(fieldId(name));
    }

    /**
     * Try finding the field by ID. Used for types with stable schema (Serializable) to avoid
     * (string -> ID) calculations.
     *
     * @param id Field ID.
     * @return {@code True} if field was found and stream was positioned accordingly.
     */
    private boolean findFieldById(int id) {
        assert !raw; // Assert, not exception, because this is called only from internals for Serializable types.
        assert dataStart != start;

        if (footerLen == 0)
            return false;

        if (userType) {
            int order;

            if (matching) {
                // Trying to get field order speculatively.
                int expOrder = matchingOrder++;

                int realId = schema.fieldId(expOrder);

                if (realId == id) {
                    if (expOrder == 0)
                        streamPosition(dataStart);

                    return true;
                }
                else {
                    // Mismatch detected, no need for further speculations.
                    matching = false;

                    order = schema.order(id);
                }
            }
            else
                order = schema.order(id);

            return trySetUserFieldPosition(order);
        }
        else
            return trySetSystemFieldPosition(id);
    }

    /**
     * Set position for the given user field order.
     *
     * @param order Order.
     * @return {@code True} if field was found and stream was positioned accordingly.
     */
    private boolean trySetUserFieldPosition(int order) {
        if (order != PortableSchema.ORDER_NOT_FOUND) {
            int offsetPos = footerStart + order * (fieldIdLen + fieldOffsetLen) + fieldIdLen;

            int pos = start + PortableUtils.fieldOffsetRelative(in, offsetPos, fieldOffsetLen);

            streamPosition(pos);

            return true;
        }
        else
            return false;
    }

    /**
     * Set position for the given system field ID.
     *
     * @param id Field ID.
     * @return {@code True} if field was found and stream was positioned accordingly.
     */
    private boolean trySetSystemFieldPosition(int id) {
        // System types are never written with compact footers because they do not have metadata.
        assert fieldIdLen == PortableUtils.FIELD_ID_LEN;

        int searchPos = footerStart;
        int searchTail = searchPos + footerLen;

        while (true) {
            if (searchPos >= searchTail)
                return false;

            int id0 = in.readIntPositioned(searchPos);

            if (id0 == id) {
                int pos = start + PortableUtils.fieldOffsetRelative(in, searchPos + PortableUtils.FIELD_ID_LEN,
                    fieldOffsetLen);

                streamPosition(pos);

                return true;
            }

            searchPos += PortableUtils.FIELD_ID_LEN + fieldOffsetLen;
        }
    }

    /**
     * Set stream position.
     *
     * @param pos Position.
     */
    private void streamPosition(int pos) {
        in.position(pos);
    }

    /**
     * Set stream position as a part of some random read. Further speculations will be disabled after this call.
     *
     * @param pos Position.
     */
    private void streamPositionRandom(int pos) {
        streamPosition(pos);

        matching = false;
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    /** {@inheritDoc} */
    @Override public String readLine() throws IOException {
        SB sb = new SB();

        int b;

        while ((b = read()) >= 0) {
            char c = (char)b;

            switch (c) {
                case '\n':
                    return sb.toString();

                case '\r':
                    b = read();

                    if (b < 0 || b == '\n')
                        return sb.toString();
                    else
                        sb.a((char)b);

                    break;

                default:
                    sb.a(c);
            }
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @NotNull @Override public String readUTF() throws IOException {
        return readString();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void readFully(byte[] b, int off, int len) throws IOException {
        int cnt = in.read(b, off, len);

        if (cnt < len)
            throw new EOFException();
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        int toSkip = Math.min(in.remaining(), n);

        streamPositionRandom(in.position() + toSkip);

        return toSkip;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        return readByte();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public long skip(long n) throws IOException {
        return skipBytes((int) n);
    }

    /** {@inheritDoc} */
    @Override public int available() throws IOException {
        return in.remaining();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        // No-op.
    }

    /**
     * Flag.
     */
    private static enum Flag {
        /** Regular. */
        NORMAL,

        /** Handle. */
        HANDLE,

        /** Null. */
        NULL
    }
}
