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

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.BINARY_ENUM;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.BINARY_OBJ;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.BOOLEAN;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.BYTE;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.CHAR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.CHAR_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.CLASS;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.COL;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DATE;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DATE_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DECIMAL;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DECIMAL_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DFLT_HDR_LEN;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DOUBLE;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DOUBLE_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.ENUM;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.ENUM_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.EXTERNALIZABLE;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.EXTERNALIZABLE_HDR_LEN;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.FLOAT;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.FLOAT_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.HANDLE;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.INT;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.INT_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.LONG;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.LONG_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.MAP;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.NULL;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.OBJ;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.OPTM_MARSH;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.PROXY;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.SHORT;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.STRING;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.STRING_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TIME;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TIMESTAMP_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TIME_ARR;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.UNREGISTERED_TYPE_ID;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.UUID;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.UUID_ARR;

/**
 * Binary reader implementation.
 */
@SuppressWarnings("unchecked")
public class BinaryReaderExImpl implements BinaryReader, BinaryRawReaderEx, BinaryReaderHandlesHolder, ObjectInput {
    /** Binary context. */
    private final BinaryContext ctx;

    /** Input stream. */
    private final BinaryInputStream in;

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

    /** Class descriptor. */
    private BinaryClassDescriptor desc;

    /** Mapper. */
    private final BinaryInternalMapper mapper;

    /** Schema Id. */
    private final int schemaId;

    /** Whether this is user type or not. */
    private final boolean userType;

    /** Whether field IDs exist. */
    private final int fieldIdLen;

    /** Offset size in bytes. */
    private final int fieldOffLen;

    /** Object schema. */
    private final BinarySchema schema;

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
     * @param forUnmarshal {@code True} if reader is needed to unmarshal object.
     */
    public BinaryReaderExImpl(BinaryContext ctx, BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal) {
        this(ctx,
            in,
            ldr,
            null,
            forUnmarshal);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    public BinaryReaderExImpl(BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean forUnmarshal) {
        this(ctx,
            in,
            ldr,
            hnds,
            false,
            forUnmarshal);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param skipHdrCheck Whether to skip header check.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    public BinaryReaderExImpl(BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean skipHdrCheck,
        boolean forUnmarshal) {
        // Initialize base members.
        this.ctx = ctx;
        this.in = in;
        this.ldr = ldr;
        this.hnds = hnds;

        start = in.position();

        byte objType = in.readByte();

        // Perform full header parsing in case of binary object.
        if (!skipHdrCheck && (objType == GridBinaryMarshaller.OBJ)) {
            // Ensure protocol is fine.
            BinaryUtils.checkProtocolVersion(in.readByte());

            // Read header content.
            short flags = in.readShort();
            int typeId0 = in.readInt();

            in.readInt(); // Skip hash code.

            int len = in.readInt();
            schemaId = in.readInt();
            int offset = in.readInt();

            // Get trivial flag values.
            userType = BinaryUtils.isUserType(flags);
            fieldIdLen = BinaryUtils.fieldIdLength(flags);
            fieldOffLen = BinaryUtils.fieldOffsetLength(flags);

            // Calculate footer borders and raw offset.
            if (BinaryUtils.hasSchema(flags)) {
                // Schema exists.
                footerStart = start + offset;

                if (BinaryUtils.hasRaw(flags)) {
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

                if (BinaryUtils.hasRaw(flags))
                    rawOff = start + offset;
                else
                    rawOff = start + len;
            }

            // Finally, we have to resolve real type ID.
            if (typeId0 == UNREGISTERED_TYPE_ID) {
                int off = in.position();

                if (forUnmarshal) {
                    // Registers class by type ID, at least locally if the cache is not ready yet.
                    desc = ctx.descriptorForClass(BinaryUtils.doReadClass(in, ctx, ldr, typeId0), false);

                    typeId = desc.typeId();
                }
                else
                    typeId = ctx.typeId(BinaryUtils.doReadClassName(in));

                int clsNameLen = in.position() - off;

                dataStart = start + DFLT_HDR_LEN + clsNameLen;
            }
            else {
                typeId = typeId0;

                dataStart = start + DFLT_HDR_LEN;
            }

            mapper = userType ? ctx.userTypeMapper(typeId) : BinaryContext.defaultMapper();
            schema = BinaryUtils.hasSchema(flags) ? getOrCreateSchema() : null;
        }
        else if (objType == GridBinaryMarshaller.EXTERNALIZABLE) {
            in.readInt(); //skip len

            int typeId0 = in.readInt();

            if (typeId0 == UNREGISTERED_TYPE_ID) {
                // Read header content.
                int off = in.position();

                if (forUnmarshal) {
                    // Registers class by type ID, at least locally if the cache is not ready yet.
                    desc = ctx.descriptorForClass(BinaryUtils.doReadClass(in, ctx, ldr, typeId0), false);

                    typeId = desc.typeId();
                }
                else
                    typeId = ctx.typeId(BinaryUtils.doReadClassName(in));

                int clsNameLen = in.position() - off;

                dataStart = start + EXTERNALIZABLE_HDR_LEN + clsNameLen;
            }
            else {
                typeId = typeId0;

                dataStart = start + EXTERNALIZABLE_HDR_LEN;
            }

            rawOff = 0;
            footerStart = 0;
            footerLen = 0;
            mapper = null;
            schemaId = 0;
            userType = false;
            fieldIdLen = 0;
            fieldOffLen = 0;
            schema = null;
        }
        else {
            dataStart = 0;
            typeId = 0;
            rawOff = 0;
            footerStart = 0;
            footerLen = 0;
            mapper = null;
            schemaId = 0;
            userType = false;
            fieldIdLen = 0;
            fieldOffLen = 0;
            schema = null;
        }

        streamPosition(start);
    }

    /**
     * @return Input stream.
     */
    public BinaryInputStream in() {
        return in;
    }

    /**
     * @return Descriptor.
     */
    BinaryClassDescriptor descriptor() {
        if (desc == null)
            desc = ctx.descriptorForTypeId(userType, typeId, ldr, true);

        return desc;
    }

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshal(int offset) throws BinaryObjectException {
        streamPosition(offset);

        return in.position() >= 0 ? BinaryUtils.unmarshal(in, ctx, ldr, this) : null;
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? BinaryUtils.unmarshal(in, ctx, ldr, this) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
    }

    /**
     * @param fieldId Field ID.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? BinaryUtils.unmarshal(in, ctx, ldr, this) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Binary object.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable BinaryObject readBinaryObject(int fieldId) throws BinaryObjectException {
        if (findFieldById(fieldId)) {
            if (checkFlag(BINARY_OBJ) == Flag.NULL)
                return null;

            return new BinaryObjectImpl(ctx, BinaryUtils.doReadByteArray(in), in.readInt());
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

            return BinaryUtils.doReadClass(in, ctx, ldr);
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
        int handlePos = BinaryUtils.positionForHandle(in) - in.readInt();

        Object obj = getHandle(handlePos);

        if (obj == null) {
            int retPos = in.position();

            streamPosition(handlePos);

            obj = BinaryUtils.doReadObject(in, ctx, ldr, this);

            streamPosition(retPos);
        }

        return (T)obj;
    }

    /**
     * Wraps an exception by adding the fieldName
     *
     * @param fieldName the name of the field, causes failure
     * @param e the cause of the deserialization failure
     * @return wrapping exception
     */
    private BinaryObjectException wrapFieldException(String fieldName, Exception e) {
        if (S.INCLUDE_SENSITIVE)
            return new BinaryObjectException("Failed to read field: " + fieldName, e);
        else
            return new BinaryObjectException("Failed to read field.", e);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(BYTE) == Flag.NORMAL ? in.readByte() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readByteArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadByteArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(BOOLEAN) == Flag.NORMAL && in.readBoolean();
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readBooleanArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadBooleanArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(SHORT) == Flag.NORMAL ? in.readShort() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readShortArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadShortArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(CHAR) == Flag.NORMAL ? in.readChar() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readCharArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadCharArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(INT) == Flag.NORMAL ? in.readInt() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readIntArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadIntArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(LONG) == Flag.NORMAL ? in.readLong() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readLongArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadLongArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(FLOAT) == Flag.NORMAL ? in.readFloat() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readFloatArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadFloatArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) && checkFlagNoHandles(DOUBLE) == Flag.NORMAL ? in.readDouble() : 0;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        try {
            return findFieldByName(fieldName) ? this.readDoubleArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadDoubleArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readDecimal() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        return checkFlagNoHandles(DECIMAL) == Flag.NORMAL ? BinaryUtils.doReadDecimal(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readDecimalArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadDecimalArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public String readString(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readString() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        return checkFlagNoHandles(STRING) == Flag.NORMAL ? BinaryUtils.doReadString(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String[] readStringArray(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readStringArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadStringArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public UUID readUuid(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readUuid() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        return checkFlagNoHandles(UUID) == Flag.NORMAL ? BinaryUtils.doReadUuid(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public UUID[] readUuidArray(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readUuidArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadUuidArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Date readDate(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readDate() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        return checkFlagNoHandles(DATE) == Flag.NORMAL ? BinaryUtils.doReadDate(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Date[] readDateArray(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readDateArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadDateArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Timestamp readTimestamp(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readTimestamp() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
        return checkFlagNoHandles(TIMESTAMP) == Flag.NORMAL ? BinaryUtils.doReadTimestamp(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Timestamp[] readTimestampArray(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readTimestampArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadTimestampArray(in);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Time readTime(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readTime() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Time readTime() throws BinaryObjectException {
        return checkFlagNoHandles(TIME) == Flag.NORMAL ? BinaryUtils.doReadTime(in) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Time[] readTimeArray(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? this.readTimeArray() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Time readTime(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readTime() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Time[] readTimeArray(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? this.readTimeArray() : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Time[] readTimeArray() throws BinaryObjectException {
        switch (checkFlag(TIME_ARR)) {
            case NORMAL:
                return BinaryUtils.doReadTimeArray(in);
            case HANDLE:
                return readHandleField();
            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T readObject(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? (T)BinaryUtils.doReadObject(in, ctx, ldr, this) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object readObject(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? BinaryUtils.doReadObject(in, ctx, ldr, this) : null;
    }

    /** {@inheritDoc} */
    @Override public Object readObject() throws BinaryObjectException {
        return BinaryUtils.doReadObject(in, ctx, ldr, this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObjectDetached() throws BinaryObjectException {
        return BinaryUtils.unmarshal(in, ctx, ldr, this, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? this.readObjectArray() : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return BinaryUtils.doReadObjectArray(in, ctx, ldr, this, true);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? (T)readEnum0(null) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
            Class<?> cls0 = BinaryUtils.doReadClass(in, ctx, ldr);

            if (cls == null)
                cls = cls0;

            return BinaryUtils.doReadEnum(in, cls);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray(String fieldName)
        throws BinaryObjectException {

        try {
            return findFieldByName(fieldName) ? (T[])readEnumArray0(null) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
    }

    /**
     * @param fieldId Field ID.
     * @return Binary Enum
     * @throws BinaryObjectException If failed.
     */
    @Nullable BinaryEnumObjectImpl readBinaryEnum(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? BinaryUtils.doReadBinaryEnum(in, ctx) : null;
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
                Class<?> cls0 = BinaryUtils.doReadClass(in, ctx, ldr);

                if (cls == null)
                    cls = cls0;

                return BinaryUtils.doReadEnumArray(in, ctx, ldr, cls);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws BinaryObjectException {
        try {
            return findFieldByName(fieldName) ? (Collection<T>)readCollection0(null) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName, BinaryCollectionFactory<T> factory)
        throws BinaryObjectException {

        try {
            return findFieldByName(fieldName) ? readCollection0(factory) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return (Collection)BinaryUtils.doReadCollection(in, ctx, ldr, this, true, factory);

            case HANDLE: {
                int handlePos = BinaryUtils.positionForHandle(in) - in.readInt();

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
        try {
            return findFieldByName(fieldName) ? (Map<K, V>)readMap0(null) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName, BinaryMapFactory<K, V> factory)
        throws BinaryObjectException {

        try {
            return findFieldByName(fieldName) ? readMap0(factory) : null;
        }
        catch (Exception ex) {
            throw wrapFieldException(fieldName, ex);
        }
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
                return (Map)BinaryUtils.doReadMap(in, ctx, ldr, this, true, factory);

            case HANDLE: {
                int handlePos = BinaryUtils.positionForHandle(in) - in.readInt();

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

        int pos = BinaryUtils.positionForHandle(in);

        throw new BinaryObjectException("Unexpected field type [pos=" + pos + ", expected=" + fieldFlagName(expFlag) +
            ", actual=" + fieldFlagName(flag) + ']');
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

        int pos = BinaryUtils.positionForHandle(in);

        throw new BinaryObjectException("Unexpected field type [pos=" + pos + ", expected=" + fieldFlagName(expFlag) +
            ", actual=" + fieldFlagName(flag) + ']');
    }

    /**
     * Gets a flag name
     *
     * @param flag a flag value
     * @return string representation of the flag (type name, handle, else number)
     */
    private String fieldFlagName(byte flag) {
        String typeName = BinaryUtils.fieldTypeName(flag);

        return typeName == null ? String.valueOf(flag) : typeName;
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
        String newName = ctx.configuration().getIgniteInstanceName();
        String oldName = IgniteUtils.setCurrentIgniteName(newName);

        try {
            return deserialize0();
        }
        finally {
            IgniteUtils.restoreOldIgniteName(oldName, newName);
        }
    }

    /**
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    @Nullable private Object deserialize0() throws BinaryObjectException {
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

                    obj = BinaryUtils.doReadObject(in, ctx, ldr, this);

                    streamPosition(retPos);
                }

                break;

            case OBJ:
                if (desc == null)
                    desc = ctx.descriptorForTypeId(userType, typeId, ldr, true);

                streamPosition(dataStart);

                if (desc == null)
                    throw new BinaryInvalidTypeException("Unknown type ID: " + typeId);

                obj = desc.read(this);

                streamPosition(footerStart + footerLen);

                break;

            case EXTERNALIZABLE:
                if (desc == null)
                    desc = ctx.descriptorForTypeId(userType, typeId, ldr, true);

                streamPosition(dataStart);

                obj = desc.read(this);

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
                obj = BinaryUtils.doReadDecimal(in);

                break;

            case STRING:
                obj = BinaryUtils.doReadString(in);

                break;

            case UUID:
                obj = BinaryUtils.doReadUuid(in);

                break;

            case DATE:
                obj = BinaryUtils.doReadDate(in);

                break;

            case TIMESTAMP:
                obj = BinaryUtils.doReadTimestamp(in);

                break;

            case TIME:
                obj = BinaryUtils.doReadTime(in);

                break;

            case BYTE_ARR:
                obj = BinaryUtils.doReadByteArray(in);

                break;

            case SHORT_ARR:
                obj = BinaryUtils.doReadShortArray(in);

                break;

            case INT_ARR:
                obj = BinaryUtils.doReadIntArray(in);

                break;

            case LONG_ARR:
                obj = BinaryUtils.doReadLongArray(in);

                break;

            case FLOAT_ARR:
                obj = BinaryUtils.doReadFloatArray(in);

                break;

            case DOUBLE_ARR:
                obj = BinaryUtils.doReadDoubleArray(in);

                break;

            case CHAR_ARR:
                obj = BinaryUtils.doReadCharArray(in);

                break;

            case BOOLEAN_ARR:
                obj = BinaryUtils.doReadBooleanArray(in);

                break;

            case DECIMAL_ARR:
                obj = BinaryUtils.doReadDecimalArray(in);

                break;

            case STRING_ARR:
                obj = BinaryUtils.doReadStringArray(in);

                break;

            case UUID_ARR:
                obj = BinaryUtils.doReadUuidArray(in);

                break;

            case DATE_ARR:
                obj = BinaryUtils.doReadDateArray(in);

                break;

            case TIMESTAMP_ARR:
                obj = BinaryUtils.doReadTimestampArray(in);

                break;

            case TIME_ARR:
                obj = BinaryUtils.doReadTimeArray(in);

                break;

            case OBJ_ARR:
                obj = BinaryUtils.doReadObjectArray(in, ctx, ldr, this, true);

                break;

            case COL:
                obj = BinaryUtils.doReadCollection(in, ctx, ldr, this, true, null);

                break;

            case MAP:
                obj = BinaryUtils.doReadMap(in, ctx, ldr, this, true, null);

                break;

            case BINARY_OBJ:
                obj = BinaryUtils.doReadBinaryObject(in, ctx, false);

                ((BinaryObjectImpl)obj).context(ctx);

                if (!GridBinaryMarshaller.KEEP_BINARIES.get())
                    obj = ((BinaryObject)obj).deserialize();

                break;

            case ENUM:
                obj = BinaryUtils.doReadEnum(in, BinaryUtils.doReadClass(in, ctx, ldr));

                break;

            case ENUM_ARR:
                obj = BinaryUtils.doReadEnumArray(in, ctx, ldr, BinaryUtils.doReadClass(in, ctx, ldr));

                break;

            case BINARY_ENUM:
                obj = BinaryUtils.doReadBinaryEnum(in, ctx);

                if (!GridBinaryMarshaller.KEEP_BINARIES.get())
                    obj = ((BinaryObject)obj).deserialize();

                break;

            case CLASS:
                obj = BinaryUtils.doReadClass(in, ctx, ldr);

                break;

            case PROXY:
                obj = BinaryUtils.doReadProxy(in, ctx, ldr, this);

                break;

            case OPTM_MARSH:
                obj = BinaryUtils.doReadOptimized(in, ctx, ldr);

                break;

            default:
                throw new BinaryObjectException("Invalid flag value: " + flag);
        }

        return obj;
    }

    /**
     * @param fieldId Field ID.
     * @return Deserialized object.
     * @throws BinaryObjectException If failed.
     */
    @Nullable Object readField(int fieldId) throws BinaryObjectException {
        if (!findFieldById(fieldId))
            return null;

        return new BinaryReaderExImpl(ctx, in, ldr, hnds, true).deserialize();
    }

    /**
     * @param name Field name.
     * @return Field offset.
     */
    private int fieldId(String name) {
        assert name != null;

        return mapper.fieldId(typeId, name);
    }

    /**
     * Get or create object schema.
     *
     * @return Schema.
     */
    public BinarySchema getOrCreateSchema() {
        BinarySchema schema = ctx.schemaRegistry(typeId).schema(schemaId);

        if (schema == null) {
            if (fieldIdLen != BinaryUtils.FIELD_ID_LEN) {
                BinaryTypeImpl type = (BinaryTypeImpl) ctx.metadata(typeId, schemaId);

                if (type == null || type.metadata() == null)
                    throw new BinaryObjectException("Cannot find metadata for object with compact footer: " +
                        typeId);

                for (BinarySchema typeSchema : type.metadata().schemas()) {
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
    private BinarySchema createSchema() {
        assert fieldIdLen == BinaryUtils.FIELD_ID_LEN;

        BinarySchema.Builder builder = BinarySchema.Builder.newBuilder();

        int searchPos = footerStart;
        int searchEnd = searchPos + footerLen;

        while (searchPos < searchEnd) {
            int fieldId = in.readIntPositioned(searchPos);

            builder.addField(fieldId);

            searchPos += BinaryUtils.FIELD_ID_LEN + fieldOffLen;
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

                BinarySchema.Confirmation confirm = schema.confirmOrder(expOrder, name);

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
                        assert confirm == BinarySchema.Confirmation.CLARIFY;

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
        if (order != BinarySchema.ORDER_NOT_FOUND) {
            int offsetPos = footerStart + order * (fieldIdLen + fieldOffLen) + fieldIdLen;

            int pos = start + BinaryUtils.fieldOffsetRelative(in, offsetPos, fieldOffLen);

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
        assert fieldIdLen == BinaryUtils.FIELD_ID_LEN;

        int searchPos = footerStart;
        int searchTail = searchPos + footerLen;

        while (true) {
            if (searchPos >= searchTail)
                return false;

            int id0 = in.readIntPositioned(searchPos);

            if (id0 == id) {
                int pos = start + BinaryUtils.fieldOffsetRelative(in, searchPos + BinaryUtils.FIELD_ID_LEN,
                    fieldOffLen);

                streamPosition(pos);

                return true;
            }

            searchPos += BinaryUtils.FIELD_ID_LEN + fieldOffLen;
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
     * @return Binary context.
     */
    public BinaryContext context() {
        return ctx;
    }

    /**
     * Flag.
     */
    private enum Flag {
        /** Regular. */
        NORMAL,

        /** Handle. */
        HANDLE,

        /** Null. */
        NULL
    }
}
