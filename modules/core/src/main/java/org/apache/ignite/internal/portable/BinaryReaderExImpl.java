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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ARR_LIST;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLASS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CONC_HASH_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CONC_SKIP_LIST_SET;
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
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HASH_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HASH_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LINKED_HASH_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LINKED_HASH_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LINKED_LIST;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP_ENTRY;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJECT_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OPTM_MARSH;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PORTABLE_OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PROPERTIES_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TREE_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TREE_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.USER_COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.USER_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID_ARR;

/**
 * Portable reader implementation.
 */
@SuppressWarnings("unchecked")
public class BinaryReaderExImpl implements BinaryReader, BinaryRawReaderEx, ObjectInput {
    /** Portable context. */
    private final PortableContext ctx;

    /** Input stream. */
    private final PortableInputStream in;

    /** Class loaded. */
    private final ClassLoader ldr;

    /** Reader context which is constantly passed between objects. */
    private final BinaryReaderHandles rCtx;

    /** */
    private final int start;

    /** Type ID. */
    private final int typeId;

    /** Raw offset. */
    private final int rawOff;

    /** */
    private final int hdrLen;

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

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param rCtx Context.
     */
    public BinaryReaderExImpl(PortableContext ctx, PortableInputStream in, ClassLoader ldr, BinaryReaderHandles rCtx) {
        // Initialize base members.
        this.ctx = ctx;
        this.in = in;
        this.ldr = ldr;
        this.rCtx = rCtx;

        start = in.position();

        // Parse header if possible.
        byte hdr = in.readBytePositioned(start);

        if (hdr == GridPortableMarshaller.OBJ) {
            // Skip header.
            in.readByte();

            // Ensure protocol is fine.
            PortableUtils.checkProtocolVersion(in.readByte());

            // Read and parse flags.
            short flags = in.readShort();

            userType = PortableUtils.isUserType(flags);

            fieldIdLen = PortableUtils.fieldIdLength(flags);
            fieldOffsetLen = PortableUtils.fieldOffsetLength(flags);

            int typeId0 = in.readIntPositioned(start + GridPortableMarshaller.TYPE_ID_POS);

            IgniteBiTuple<Integer, Integer> footer = PortableUtils.footerAbsolute(in, start);

            footerStart = footer.get1();
            footerLen = footer.get2() - footerStart;

            schemaId = in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_ID_POS);

            rawOff = PortableUtils.rawOffsetAbsolute(in, start);

            if (typeId0 == UNREGISTERED_TYPE_ID) {
                // Skip to the class name position.
                in.position(start + GridPortableMarshaller.DFLT_HDR_LEN);

                int off = in.position();

                Class cls = doReadClass(typeId0);

                // registers class by typeId, at least locally if the cache is not ready yet.
                PortableClassDescriptor desc = ctx.descriptorForClass(cls);

                typeId = desc.typeId();

                int clsNameLen = in.position() - off;

                hdrLen = DFLT_HDR_LEN + clsNameLen;
            }
            else {
                typeId = typeId0;

                hdrLen = DFLT_HDR_LEN;
            }

            idMapper = userType ? ctx.userTypeIdMapper(typeId) : null;
            schema = PortableUtils.hasSchema(flags) ? getOrCreateSchema() : null;

            in.position(start);
        }
        else {
            typeId = 0;
            rawOff = 0;
            hdrLen = 0;
            footerStart = 0;
            footerLen = 0;
            idMapper = null;
            schemaId = 0;
            userType = false;
            fieldIdLen = 0;
            fieldOffsetLen = 0;
            schema = null;
        }
    }

    /**
     * @return Handles.
     */
    public BinaryReaderHandles handles() {
        return rCtx;
    }

    /**
     * @return Descriptor.
     */
    PortableClassDescriptor descriptor() {
        return ctx.descriptorForTypeId(userType, typeId, ldr);
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshal() throws BinaryObjectException {
        return unmarshal(false);
    }

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    public Object unmarshal(int offset) throws BinaryObjectException {
        // Random reads prevent any further speculations.
        matching = false;

        in.position(offset);

        return in.position() >= 0 ? unmarshal() : null;
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(String fieldName) throws BinaryObjectException {
        return hasField(fieldName) ? unmarshal() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? unmarshal() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException On case of error.
     */
    @Nullable Map.Entry<?, ?> readMapEntry(int fieldId) throws BinaryObjectException {
        if (findFieldById(fieldId)) {
            Flag flag = checkFlag(MAP_ENTRY);

            if (flag == Flag.NORMAL)
                return doReadMapEntry(true);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
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

            return new BinaryObjectImpl(ctx, doReadByteArray(), in.readInt());
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

            return doReadClass();
        }

        return null;
    }

    /**
     * @param obj Object.
     */
    void setHandler(Object obj) {
        rCtx.put(start, obj);
    }

    /**
     * @param obj Object.
     * @param pos Position.
     */
    void setHandler(Object obj, int pos) {
        rCtx.put(pos, obj);
    }

    /**
     * Recreating field value from a handle.
     *
     * @param <T> Field type.
     * @return Field.
     */
    private <T> T readHandleField() {
        int handle = (in.position() - 1) - in.readInt();

        int retPos = in.position();

        Object obj = rCtx.get(handle);

        if (obj == null) {
            in.position(handle);

            obj = doReadObject();

            in.position(retPos);
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
                return doReadByteArray();

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
                return doReadBooleanArray();

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
                return doReadShortArray();

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
                return doReadCharArray();

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
                return doReadIntArray();

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
                return doReadLongArray();

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
                return doReadFloatArray();

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
                return doReadDoubleArray();

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
        return checkFlagNoHandles(DECIMAL) == Flag.NORMAL ? doReadDecimal() : null;
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
                return doReadDecimalArray();

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
        return checkFlagNoHandles(STRING) == Flag.NORMAL ? doReadString() : null;
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
                return doReadStringArray();

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
        return checkFlagNoHandles(UUID) == Flag.NORMAL ? doReadUuid() : null;
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
                return doReadUuidArray();

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
        return checkFlagNoHandles(DATE) == Flag.NORMAL ? doReadDate() : null;
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
                return doReadDateArray();

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
        return checkFlagNoHandles(TIMESTAMP) == Flag.NORMAL ? doReadTimestamp() : null;
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
                return doReadTimestampArray();

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T readObject(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? (T)doReadObject() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object readObject(int fieldId) throws BinaryObjectException {
        return findFieldById(fieldId) ? doReadObject() : null;
    }

    /** {@inheritDoc} */
    @Override public Object readObject() throws BinaryObjectException {
        return doReadObject();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObjectDetached() throws BinaryObjectException {
        return unmarshal(true);
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
                return doReadObjectArray(true);

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
            Class<?> cls0 = doReadClass();

            if (cls == null)
                cls = cls0;

            return doReadEnum(cls);
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
                Class<?> cls0 = doReadClass();

                if (cls == null)
                    cls = cls0;

                return doReadEnumArray(cls);

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
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName,
        Class<? extends Collection<T>> colCls) throws BinaryObjectException {
        return findFieldByName(fieldName) ? readCollection0(colCls) : null;
    }

    /**
     * @param fieldId Field ID.
     * @param colCls Collection class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable <T> Collection<T> readCollection(int fieldId, @Nullable Class<? extends Collection> colCls)
        throws BinaryObjectException {
        return findFieldById(fieldId) ? (Collection<T>)readCollection0(colCls) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws BinaryObjectException {
        return readCollection0(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(Class<? extends Collection<T>> colCls)
        throws BinaryObjectException {
        return readCollection0(colCls);
    }

    /**
     * Internal read collection routine.
     *
     * @param cls Collection class.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    private Collection readCollection0(@Nullable Class<? extends Collection> cls)
        throws BinaryObjectException {
        switch (checkFlag(COL)) {
            case NORMAL:
                return (Collection)doReadCollection(true, cls);

            case HANDLE:
                return readHandleField();

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws BinaryObjectException {
        return findFieldByName(fieldName) ? (Map<K, V>)readMap0(null) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws BinaryObjectException {
        return findFieldByName(fieldName) ? readMap0(mapCls) : null;
    }

    /**
     * @param fieldId Field ID.
     * @param mapCls Map class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Map<?, ?> readMap(int fieldId, @Nullable Class<? extends Map> mapCls) throws BinaryObjectException {
        return findFieldById(fieldId) ? readMap0(mapCls) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws BinaryObjectException {
        return readMap0(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(Class<? extends Map<K, V>> mapCls)
        throws BinaryObjectException {
        return readMap0(mapCls);
    }

    /**
     * Internal read map routine.
     *
     * @param cls Map class.
     * @return Value.
     * @throws BinaryObjectException If failed.
     */
    private Map readMap0(@Nullable Class<? extends Map> cls) throws BinaryObjectException {
        switch (checkFlag(MAP)) {
            case NORMAL:
                return (Map)doReadMap(true, cls);

            case HANDLE:
                return readHandleField();

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

        int pos = positionForHandle();

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

        int pos = positionForHandle();

        throw new BinaryObjectException("Unexpected flag value [pos=" + pos + ", expected=" + expFlag +
            ", actual=" + flag + ']');
    }

    /**
     * @param fieldName Field name.
     * @return {@code True} if field is set.
     */
    public boolean hasField(String fieldName) {
        return findFieldById(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public BinaryRawReader rawReader() {
        in.position(rawOff);

        return this;
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable private Object unmarshal(boolean detach) throws BinaryObjectException {
        int start = in.position();

        byte flag = in.readByte();

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = start - in.readInt();

                BinaryObject handledPo = rCtx.get(handle);

                if (handledPo != null)
                    return handledPo;

                in.position(handle);

                return unmarshal();

            case OBJ:
                PortableUtils.checkProtocolVersion(in.readByte());

                BinaryObjectEx po;

                if (detach) {
                    in.position(start + GridPortableMarshaller.TOTAL_LEN_POS);

                    int len = in.readInt();

                    in.position(start);

                    po = new BinaryObjectImpl(ctx, in.readByteArray(len), 0);
                }
                else
                    po = in.offheapPointer() > 0
                        ? new BinaryObjectOffheapImpl(ctx, in.offheapPointer(), start,
                        in.remaining() + in.position())
                        : new BinaryObjectImpl(ctx, in.array(), start);

                rCtx.put(start, po);

                in.position(start + po.length());

                return po;

            case BYTE:
                return in.readByte();

            case SHORT:
                return in.readShort();

            case INT:
                return in.readInt();

            case LONG:
                return in.readLong();

            case FLOAT:
                return in.readFloat();

            case DOUBLE:
                return in.readDouble();

            case CHAR:
                return in.readChar();

            case BOOLEAN:
                return in.readBoolean();

            case DECIMAL:
                return doReadDecimal();

            case STRING:
                return doReadString();

            case UUID:
                return doReadUuid();

            case DATE:
                return doReadDate();

            case TIMESTAMP:
                return doReadTimestamp();

            case BYTE_ARR:
                return doReadByteArray();

            case SHORT_ARR:
                return doReadShortArray();

            case INT_ARR:
                return doReadIntArray();

            case LONG_ARR:
                return doReadLongArray();

            case FLOAT_ARR:
                return doReadFloatArray();

            case DOUBLE_ARR:
                return doReadDoubleArray();

            case CHAR_ARR:
                return doReadCharArray();

            case BOOLEAN_ARR:
                return doReadBooleanArray();

            case DECIMAL_ARR:
                return doReadDecimalArray();

            case STRING_ARR:
                return doReadStringArray();

            case UUID_ARR:
                return doReadUuidArray();

            case DATE_ARR:
                return doReadDateArray();

            case TIMESTAMP_ARR:
                return doReadTimestampArray();

            case OBJ_ARR:
                return doReadObjectArray(false);

            case COL:
                return doReadCollection(false, null);

            case MAP:
                return doReadMap(false, null);

            case MAP_ENTRY:
                return doReadMapEntry(false);

            case PORTABLE_OBJ:
                return doReadPortableObject();

            case ENUM:
                return doReadEnum(doReadClass());

            case ENUM_ARR:
                return doReadEnumArray(doReadClass());

            case CLASS:
                return doReadClass();

            case OPTM_MARSH:
                int len = in.readInt();

                ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

                Object obj;

                try {
                    obj = ctx.optimizedMarsh().unmarshal(input, null);
                }
                catch (IgniteCheckedException e) {
                    throw new BinaryObjectException("Failed to unmarshal object with optmMarsh marshaller", e);
                }

                return obj;

            default:
                throw new BinaryObjectException("Invalid flag value: " + flag);
        }
    }

    /**
     * @return Value.
     */
    private BigDecimal doReadDecimal() {
        int scale = in.readInt();
        byte[] mag = doReadByteArray();

        BigInteger intVal = new BigInteger(mag);

        if (scale < 0) {
            scale &= 0x7FFFFFFF;

            intVal = intVal.negate();
        }

        return new BigDecimal(intVal, scale);
    }

    /**
     * @return Value.
     */
    private String doReadString() {
        if (!in.hasArray())
            return new String(doReadByteArray(), UTF_8);

        int strLen = in.readInt();
        int strOff = in.position();

        String res = new String(in.array(), strOff, strLen, UTF_8);

        in.position(strOff + strLen);

        return res;
    }

    /**
     * @return Value.
     */
    private UUID doReadUuid() {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * @return Value.
     */
    private Date doReadDate() {
        long time = in.readLong();

        return new Date(time);
    }

    /**
     * @return Value.
     */
    private Timestamp doReadTimestamp() {
        long time = in.readLong();
        int nanos = in.readInt();

        Timestamp ts = new Timestamp(time);

        ts.setNanos(ts.getNanos() + nanos);

        return ts;
    }

    /**
     * @return Object.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable private Object doReadObject() throws BinaryObjectException {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx, in, ldr, rCtx);

        return reader.deserialize();
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
                int handle = start - in.readInt();

                obj = rCtx.get(handle);

                if (obj == null) {
                    int retPos = in.position();

                    in.position(handle);

                    obj = doReadObject();

                    in.position(retPos);
                }

                break;

            case OBJ:
                PortableClassDescriptor desc = ctx.descriptorForTypeId(userType, typeId, ldr);

                in.position(start + hdrLen);

                if (desc == null)
                    throw new BinaryInvalidTypeException("Unknown type ID: " + typeId);

                obj = desc.read(this);

                in.position(footerStart + footerLen);

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
                obj = doReadDecimal();

                break;

            case STRING:
                obj = doReadString();

                break;

            case UUID:
                obj = doReadUuid();

                break;

            case DATE:
                obj = doReadDate();

                break;

            case TIMESTAMP:
                obj = doReadTimestamp();

                break;

            case BYTE_ARR:
                obj = doReadByteArray();

                break;

            case SHORT_ARR:
                obj = doReadShortArray();

                break;

            case INT_ARR:
                obj = doReadIntArray();

                break;

            case LONG_ARR:
                obj = doReadLongArray();

                break;

            case FLOAT_ARR:
                obj = doReadFloatArray();

                break;

            case DOUBLE_ARR:
                obj = doReadDoubleArray();

                break;

            case CHAR_ARR:
                obj = doReadCharArray();

                break;

            case BOOLEAN_ARR:
                obj = doReadBooleanArray();

                break;

            case DECIMAL_ARR:
                obj = doReadDecimalArray();

                break;

            case STRING_ARR:
                obj = doReadStringArray();

                break;

            case UUID_ARR:
                obj = doReadUuidArray();

                break;

            case DATE_ARR:
                obj = doReadDateArray();

                break;

            case TIMESTAMP_ARR:
                obj = doReadTimestampArray();

                break;

            case OBJ_ARR:
                obj = doReadObjectArray(true);

                break;

            case COL:
                obj = doReadCollection(true, null);

                break;

            case MAP:
                obj = doReadMap(true, null);

                break;

            case MAP_ENTRY:
                obj = doReadMapEntry(true);

                break;

            case PORTABLE_OBJ:
                obj = doReadPortableObject();

                ((BinaryObjectImpl)obj).context(ctx);

                if (!GridPortableMarshaller.KEEP_PORTABLES.get())
                    obj = ((BinaryObject)obj).deserialize();

                break;

            case ENUM:
                obj = doReadEnum(doReadClass());

                break;

            case ENUM_ARR:
                obj = doReadEnumArray(doReadClass());

                break;

            case CLASS:
                obj = doReadClass();

                break;

            case OPTM_MARSH:
                int dataLen = in.readInt();

                ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), dataLen);

                try {
                    obj = ctx.optimizedMarsh().unmarshal(input, null);
                }
                catch (IgniteCheckedException e) {
                    throw new BinaryObjectException("Failed to unmarshal object with optimized marshaller", e);
                }

                in.position(in.position() + dataLen);

                break;

            default:
                throw new BinaryObjectException("Invalid flag value: " + flag);
        }

        return obj;
    }

    /**
     * @return Value.
     */
    private byte[] doReadByteArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        byte[] arr = in.readByteArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private short[] doReadShortArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        short[] arr = in.readShortArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private int[] doReadIntArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        int[] arr = in.readIntArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private long[] doReadLongArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        long[] arr = in.readLongArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private float[] doReadFloatArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        float[] arr = in.readFloatArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private double[] doReadDoubleArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        double[] arr = in.readDoubleArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private char[] doReadCharArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        char[] arr = in.readCharArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private boolean[] doReadBooleanArray() {
        int hPos = positionForHandle();

        int len = in.readInt();

        boolean[] arr = in.readBooleanArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private BigDecimal[] doReadDecimalArray() throws BinaryObjectException {
        int hPos = positionForHandle();

        int len = in.readInt();

        BigDecimal[] arr = new BigDecimal[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != DECIMAL)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadDecimal();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private String[] doReadStringArray() throws BinaryObjectException {
        int hPos = positionForHandle();

        int len = in.readInt();

        String[] arr = new String[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != STRING)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadString();
            }
        }

        return arr;
    }
    
    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private UUID[] doReadUuidArray() throws BinaryObjectException {
        int hPos = positionForHandle();

        int len = in.readInt();

        UUID[] arr = new UUID[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != UUID)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadUuid();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private Date[] doReadDateArray() throws BinaryObjectException {
        int hPos = positionForHandle();

        int len = in.readInt();

        Date[] arr = new Date[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != DATE)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadDate();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private Timestamp[] doReadTimestampArray() throws BinaryObjectException {
        int hPos = positionForHandle();

        int len = in.readInt();

        Timestamp[] arr = new Timestamp[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != TIMESTAMP)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadTimestamp();
            }
        }

        return arr;
    }

    /**
     * @param deep Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private Object[] doReadObjectArray(boolean deep) throws BinaryObjectException {
        int hPos = positionForHandle();

        Class compType = doReadClass();

        int len = in.readInt();

        Object[] arr = deep ? (Object[])Array.newInstance(compType, len) : new Object[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++)
            arr[i] = deep ? doReadObject() : unmarshal();

        return arr;
    }

    /**
     * @param deep Deep flag.
     * @param cls Collection class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Collection<?> doReadCollection(boolean deep, @Nullable Class<? extends Collection> cls)
        throws BinaryObjectException {
        int hPos = positionForHandle();

        int size = in.readInt();

        assert size >= 0;

        byte colType = in.readByte();

        Collection<Object> col;

        if (cls != null) {
            try {
                Constructor<? extends Collection> cons = cls.getConstructor();

                col = cons.newInstance();
            }
            catch (NoSuchMethodException ignored) {
                throw new BinaryObjectException("Collection class doesn't have public default constructor: " +
                    cls.getName());
            }
            catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new BinaryObjectException("Failed to instantiate collection: " + cls.getName(), e);
            }
        }
        else {
            switch (colType) {
                case ARR_LIST:
                    col = new ArrayList<>(size);

                    break;

                case LINKED_LIST:
                    col = new LinkedList<>();

                    break;

                case HASH_SET:
                    col = U.newHashSet(size);

                    break;

                case LINKED_HASH_SET:
                    col = U.newLinkedHashSet(size);

                    break;

                case TREE_SET:
                    col = new TreeSet<>();

                    break;

                case CONC_SKIP_LIST_SET:
                    col = new ConcurrentSkipListSet<>();

                    break;

                case USER_SET:
                    col = U.newHashSet(size);

                    break;

                case USER_COL:
                    col = new ArrayList<>(size);

                    break;

                default:
                    throw new BinaryObjectException("Invalid collection type: " + colType);
            }
        }

        setHandler(col, hPos);

        for (int i = 0; i < size; i++)
            col.add(deep ? doReadObject() : unmarshal());

        return col;
    }

    /**
     * @param deep Deep flag.
     * @param cls Map class.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Map<?, ?> doReadMap(boolean deep, @Nullable Class<? extends Map> cls)
        throws BinaryObjectException {
        int hPos = positionForHandle();

        int size = in.readInt();

        assert size >= 0;

        byte mapType = in.readByte();

        Map<Object, Object> map;

        if (cls != null) {
            try {
                Constructor<? extends Map> cons = cls.getConstructor();

                map = cons.newInstance();
            }
            catch (NoSuchMethodException ignored) {
                throw new BinaryObjectException("Map class doesn't have public default constructor: " +
                    cls.getName());
            }
            catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new BinaryObjectException("Failed to instantiate map: " + cls.getName(), e);
            }
        }
        else {
            switch (mapType) {
                case HASH_MAP:
                    map = U.newHashMap(size);

                    break;

                case LINKED_HASH_MAP:
                    map = U.newLinkedHashMap(size);

                    break;

                case TREE_MAP:
                    map = new TreeMap<>();

                    break;

                case CONC_HASH_MAP:
                    map = new ConcurrentHashMap<>(size);

                    break;

                case USER_COL:
                    map = U.newHashMap(size);

                    break;

                case PROPERTIES_MAP:
                    map = new Properties();

                    break;

                default:
                    throw new BinaryObjectException("Invalid map type: " + mapType);
            }
        }

        setHandler(map, hPos);

        for (int i = 0; i < size; i++)
            map.put(deep ? doReadObject() : unmarshal(), deep ? doReadObject() : unmarshal());

        return map;
    }

    /**
     * @param deep Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private Map.Entry<?, ?> doReadMapEntry(boolean deep) throws BinaryObjectException {
        int hPos = positionForHandle();

        Object val1 = deep ? doReadObject() : unmarshal();
        Object val2 = deep ? doReadObject() : unmarshal();

        GridMapEntry entry = new GridMapEntry<>(val1, val2);

        setHandler(entry, hPos);

        return entry;
    }

    /**
     * @return Value.
     */
    private BinaryObject doReadPortableObject() {
        if (in.offheapPointer() > 0) {
            int len = in.readInt();

            int pos = in.position();

            in.position(in.position() + len);

            int start = in.readInt();

            return new BinaryObjectOffheapImpl(ctx, in.offheapPointer() + pos, start, len);
        }
        else {
            byte[] arr = doReadByteArray();
            int start = in.readInt();

            return new BinaryObjectImpl(ctx, arr, start);
        }
    }

    /**
     * Having target class in place we simply read ordinal and create final representation.
     *
     * @param cls Enum class.
     * @return Value.
     */
    private Enum<?> doReadEnum(Class<?> cls) throws BinaryObjectException {
        assert cls != null;

        if (!cls.isEnum())
            throw new BinaryObjectException("Class does not represent enum type: " + cls.getName());

        int ord = in.readInt();

        return BinaryEnumCache.get(cls, ord);
    }

    /**
     * @param cls Enum class.
     * @return Value.
     */
    private Object[] doReadEnumArray(Class<?> cls) throws BinaryObjectException {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(cls, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else
                arr[i] = doReadEnum(doReadClass());
        }

        return arr;
    }

    /**
     * @return Value.
     */
    private Class doReadClass() throws BinaryObjectException {
        return doReadClass(in.readInt());
    }

    /**
     * @param typeId Type id.
     * @return Value.
     */
    private Class doReadClass(int typeId) throws BinaryObjectException {
        Class cls;

        if (typeId == OBJECT_TYPE_ID)
            return Object.class;

        if (typeId != UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr).describedClass();
        else {
            byte flag = in.readByte();

            if (flag != STRING)
                throw new BinaryObjectException("No class definition for typeId: " + typeId);

            String clsName = doReadString();

            try {
                cls = U.forName(clsName, ldr);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            // forces registering of class by type id, at least locally
            ctx.descriptorForClass(cls);
        }

        return cls;
    }

    /**
     * Get position to be used for handle. We assume here that the hdr byte was read, hence subtract -1.  
     *
     * @return Position for handle.
     */
    int positionForHandle() {
        return in.position() - 1;
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
    private boolean findFieldByName(String name) {
        assert hdrLen != 0;

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
                        order = expOrder;

                        break;

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

                            order = expOrder;
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
     * @return Offset.
     */
    private boolean findFieldById(int id) {
        assert hdrLen != 0;

        if (footerLen == 0)
            return false;

        if (userType) {
            int order;

            if (matching) {
                // Trying to get field order speculatively.
                int expOrder = matchingOrder++;

                int realId = schema.fieldId(expOrder);

                if (realId == id)
                    order = expOrder;
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
     * Set position for the given user field order and return it.
     *
     * @param order Order.
     * @return Position.
     */
    private boolean trySetUserFieldPosition(int order) {
        if (order != PortableSchema.ORDER_NOT_FOUND) {
            int offsetPos = footerStart + order * (fieldIdLen + fieldOffsetLen) + fieldIdLen;

            int pos = start + PortableUtils.fieldOffsetRelative(in, offsetPos, fieldOffsetLen);

            in.position(pos);

            return true;
        }
        else
            return false;
    }

    /**
     * Set position for the given system field ID and return it.
     *
     * @param id Field ID.
     * @return Position.
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

                in.position(pos);

                return true;
            }

            searchPos += PortableUtils.FIELD_ID_LEN + fieldOffsetLen;
        }
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

        in.position(in.position() + toSkip);

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
