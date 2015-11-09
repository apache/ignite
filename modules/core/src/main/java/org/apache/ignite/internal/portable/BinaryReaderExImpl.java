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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryTypeIdMapper;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.internal.portable.streams.PortableHeapInputStream;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.util.GridEnumCache;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    /** Length of a single field descriptor. */
    private static final int FIELD_DESC_LEN = 16;

    /** */
    private final PortableContext ctx;

    /** */
    private final PortableInputStream in;

    /** */
    private final int start;

    /** */
    private final PortableReaderContext rCtx;

    /** */
    private final ClassLoader ldr;

    /** */
    private PortableClassDescriptor desc;

    /** Flag indicating that object header was parsed. */
    private boolean hdrParsed;

    /** Type ID. */
    private int typeId;

    /** Raw offset. */
    private int rawOff;

    /** */
    private int hdrLen;

    /** Footer start. */
    private int footerStart;

    /** Footer end. */
    private int footerLen;

    /** ID mapper. */
    private BinaryTypeIdMapper idMapper;

    /** Schema Id. */
    private int schemaId;

    /** Offset size in bytes. */
    private int offsetSize;

    /** Object schema. */
    private PortableSchema schema;

    /**
     * @param ctx Context.
     * @param arr Array.
     * @param start Start.
     * @param ldr Class loader.
     */
    public BinaryReaderExImpl(PortableContext ctx, byte[] arr, int start, ClassLoader ldr) {
        this(ctx, new PortableHeapInputStream(arr), start, ldr, new PortableReaderContext());
    }

    /**
     * @param ctx Context.
     * @param in Input stream.
     * @param start Start.
     */
    BinaryReaderExImpl(PortableContext ctx, PortableInputStream in, int start, ClassLoader ldr) {
        this(ctx, in, start, ldr, new PortableReaderContext());
    }

    /**
     * @param ctx Context.
     * @param in Input stream.
     * @param start Start.
     * @param rCtx Context.
     */
    BinaryReaderExImpl(PortableContext ctx, PortableInputStream in, int start, ClassLoader ldr,
        PortableReaderContext rCtx) {
        this.ctx = ctx;
        this.in = in;
        this.start = start;
        this.ldr = ldr;
        this.rCtx = rCtx;

        in.position(start);
    }

    /**
     * Preloads typeId from the input array.
     */
    private void parseHeaderIfNeeded() {
        if (hdrParsed)
            return;

        int retPos = in.position();

        in.position(start);

        byte hdr = in.readByte();

        if (hdr != GridPortableMarshaller.OBJ)
            throw new BinaryObjectException("Invalid header [pos=" + retPos + "expected=" + GridPortableMarshaller.OBJ +
                ", actual=" + hdr + ']');

        PortableUtils.checkProtocolVersion(in.readByte());

        short flags = in.readShort();

        offsetSize = PortableUtils.fieldOffsetSize(flags);

        typeId = in.readIntPositioned(start + GridPortableMarshaller.TYPE_ID_POS);

        IgniteBiTuple<Integer, Integer> footer = PortableUtils.footerAbsolute(in, start, offsetSize);

        footerStart = footer.get1();
        footerLen = footer.get2() - footerStart;

        schemaId = in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_ID_POS);

        rawOff = PortableUtils.rawOffsetAbsolute(in, start, offsetSize);

        if (typeId == UNREGISTERED_TYPE_ID) {
            // Skip to the class name position.
            in.position(start + GridPortableMarshaller.DFLT_HDR_LEN);

            int off = in.position();

            Class cls = doReadClass(typeId);

            // registers class by typeId, at least locally if the cache is not ready yet.
            PortableClassDescriptor desc = ctx.descriptorForClass(cls);

            typeId = desc.typeId();

            int clsNameLen = in.position() - off;

            hdrLen = DFLT_HDR_LEN + clsNameLen;
        }
        else
            hdrLen = DFLT_HDR_LEN;

        // Restore state.
        in.position(retPos);

        hdrParsed = true;
    }

    /**
     * @return Descriptor.
     */
    PortableClassDescriptor descriptor() {
        return desc;
    }

    /**
     * @return Unmarshalled value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Object unmarshal() throws BinaryObjectException {
        return unmarshal(false);
    }

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public Object unmarshal(int offset) throws BinaryObjectException {
        in.position(offset);

        return in.position() >= 0 ? unmarshal() : null;
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(String fieldName) throws BinaryObjectException {
        return hasField(fieldName) ? unmarshal() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Unmarshalled value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalField(int fieldId) throws BinaryObjectException {
        parseHeaderIfNeeded();

        return hasField(fieldId) ? unmarshal() : null;
    }

    /**
     * Unmarshal field by absolute position.
     *
     * @param pos Absolute position.
     * @return Field value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable Object unmarshalFieldByAbsolutePosition(int pos) throws BinaryObjectException {
        parseHeaderIfNeeded();

        in.position(pos);

        return unmarshal();
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Byte readByte(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(BYTE) == Flag.NULL)
                return null;

            return in.readByte();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Short readShort(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(SHORT) == Flag.NULL)
                return null;

            return in.readShort();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Integer readInt(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(INT) == Flag.NULL)
                return null;

            return in.readInt();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Long readLong(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(LONG) == Flag.NULL)
                return null;

            return in.readLong();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Float readFloat(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(FLOAT) == Flag.NULL)
                return null;

            return in.readFloat();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Double readDouble(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(DOUBLE) == Flag.NULL)
                return null;

            return in.readDouble();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Character readChar(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(CHAR) == Flag.NULL)
                return null;

            return in.readChar();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Boolean readBoolean(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(BOOLEAN) == Flag.NULL)
                return null;

            return in.readBoolean();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable BigDecimal readDecimal(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(DECIMAL) == Flag.NULL)
                return null;

            return doReadDecimal();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable String readString(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(STRING) == Flag.NULL)
                return null;

            return doReadString();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable UUID readUuid(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(UUID) == Flag.NULL)
                return null;

            return doReadUuid();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Date readDate(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(DATE) == Flag.NULL)
                return null;

            return doReadDate();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Timestamp readTimestamp(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(TIMESTAMP) == Flag.NULL)
                return null;

            return doReadTimestamp();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Object readObject(int fieldId) throws BinaryObjectException {
        return hasField(fieldId) ? doReadObject() : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable byte[] readByteArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(BYTE_ARR);

            if (flag == Flag.NORMAL)
                return doReadByteArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable short[] readShortArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(SHORT_ARR);

            if (flag == Flag.NORMAL)
                return doReadShortArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable int[] readIntArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(INT_ARR);

            if (flag == Flag.NORMAL)
                return doReadIntArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable long[] readLongArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(LONG_ARR);

            if (flag == Flag.NORMAL)
                return doReadLongArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable float[] readFloatArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(FLOAT_ARR);

            if (flag == Flag.NORMAL)
                return doReadFloatArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable double[] readDoubleArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(DOUBLE_ARR);

            if (flag == Flag.NORMAL)
                return doReadDoubleArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable char[] readCharArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(CHAR_ARR);

            if (flag == Flag.NORMAL)
                return doReadCharArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable boolean[] readBooleanArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(BOOLEAN_ARR);

            if (flag == Flag.NORMAL)
                return doReadBooleanArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable BigDecimal[] readDecimalArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(DECIMAL_ARR);

            if (flag == Flag.NORMAL)
                return doReadDecimalArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable String[] readStringArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(STRING_ARR);

            if (flag == Flag.NORMAL)
                return doReadStringArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable UUID[] readUuidArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(UUID_ARR);

            if (flag == Flag.NORMAL)
                return doReadUuidArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Date[] readDateArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(DATE_ARR);

            if (flag == Flag.NORMAL)
                return doReadDateArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Timestamp[] readTimestampArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(TIMESTAMP_ARR);

            if (flag == Flag.NORMAL)
                return doReadTimestampArray();
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Object[] readObjectArray(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(OBJ_ARR);

            if (flag == Flag.NORMAL)
                return doReadObjectArray(true);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Collection class.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable <T> Collection<T> readCollection(int fieldId, @Nullable Class<? extends Collection> cls)
        throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(COL);

            if (flag == Flag.NORMAL)
                return (Collection<T>)doReadCollection(true, cls);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Map class.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Map<?, ?> readMap(int fieldId, @Nullable Class<? extends Map> cls)
        throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(MAP);

            if (flag == Flag.NORMAL)
                return doReadMap(true, cls);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException On case of error.
     */
    @Nullable Map.Entry<?, ?> readMapEntry(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable BinaryObject readPortableObject(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(PORTABLE_OBJ) == Flag.NULL)
                return null;

            return new BinaryObjectImpl(ctx, doReadByteArray(), in.readInt());
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Class.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Enum<?> readEnum(int fieldId, @Nullable Class<?> cls) throws BinaryObjectException {
        if (hasField(fieldId)) {
            if (checkFlag(ENUM) == Flag.NULL)
                return null;

            // Revisit: why have we started writing Class for enums in their serialized form?
            if (cls == null)
                cls = doReadClass();
            else
                doReadClass();

            Object[] vals = GridEnumCache.get(cls);

            return (Enum<?>)vals[in.readInt()];
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Class.
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Object[] readEnumArray(int fieldId, @Nullable Class<?> cls) throws BinaryObjectException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(ENUM_ARR);

            if (flag == Flag.NORMAL) {
                if (cls == null)
                    cls = doReadClass();
                else
                    doReadClass();

                return doReadEnumArray(cls);
            }
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Field class.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable Class<?> readClass(int fieldId) throws BinaryObjectException {
        if (hasField(fieldId)) {
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
        rCtx.setObjectHandler(start, obj);
    }

    /**
     * @param obj Object.
     * @param pos Position.
     */
    void setHandler(Object obj, int pos) {
        rCtx.setObjectHandler(pos, obj);
    }

    /**
     * Recreating field value from a handle.
     *
     * @param <T> Field type.
     * @return Field.
     */
    private <T> T readHandleField() {
        int handle = (in.position() - 1) - in.readInt();

        Object obj = rCtx.getObjectByHandle(handle);

        if (obj == null) {
            in.position(handle);

            obj = doReadObject();
        }

        return (T)obj;
    }
    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws BinaryObjectException {
        Byte val = readByte(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws BinaryObjectException {
        return in.readByte();
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws BinaryObjectException {
        Short val = readShort(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws BinaryObjectException {
        return in.readShort();
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws BinaryObjectException {
        Integer val = readInt(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws BinaryObjectException {
        return in.readInt();
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws BinaryObjectException {
        Long val = readLong(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws BinaryObjectException {
        return in.readLong();
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws BinaryObjectException {
        Float val = readFloat(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws BinaryObjectException {
        return in.readFloat();
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws BinaryObjectException {
        Double val = readDouble(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws BinaryObjectException {
        return in.readDouble();
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws BinaryObjectException {
        Character val = readChar(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws BinaryObjectException {
        return in.readChar();
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws BinaryObjectException {
        Boolean val = readBoolean(fieldId(fieldName));

        return val != null ? val : false;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws BinaryObjectException {
        return in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal(String fieldName) throws BinaryObjectException {
        return readDecimal(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal() throws BinaryObjectException {
        if (checkFlag(DECIMAL) == Flag.NULL)
            return null;

        return doReadDecimal();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws BinaryObjectException {
        return readString(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws BinaryObjectException {
        if (checkFlag(STRING) == Flag.NULL)
            return null;

        return doReadString();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws BinaryObjectException {
        return readUuid(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws BinaryObjectException {
        if (checkFlag(UUID) == Flag.NULL)
            return null;

        return doReadUuid();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate(String fieldName) throws BinaryObjectException {
        return readDate(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate() throws BinaryObjectException {
        if (checkFlag(DATE) == Flag.NULL)
            return null;

        return doReadDate();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp(String fieldName) throws BinaryObjectException {
        return readTimestamp(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp() throws BinaryObjectException {
        if (checkFlag(TIMESTAMP) == Flag.NULL)
            return null;

        return doReadTimestamp();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T readObject(String fieldName) throws BinaryObjectException {
        return (T)readObject(fieldId(fieldName));
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
    @Nullable @Override public byte[] readByteArray(String fieldName) throws BinaryObjectException {
        return readByteArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws BinaryObjectException {
        if (checkFlag(BYTE_ARR) == Flag.NULL)
            return null;

        return doReadByteArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws BinaryObjectException {
        return readShortArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws BinaryObjectException {
        if (checkFlag(SHORT_ARR) == Flag.NULL)
            return null;

        return doReadShortArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws BinaryObjectException {
        return readIntArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws BinaryObjectException {
        if (checkFlag(INT_ARR) == Flag.NULL)
            return null;

        return doReadIntArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws BinaryObjectException {
        return readLongArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws BinaryObjectException {
        if (checkFlag(LONG_ARR) == Flag.NULL)
            return null;

        return doReadLongArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws BinaryObjectException {
        return readFloatArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws BinaryObjectException {
        if (checkFlag(FLOAT_ARR) == Flag.NULL)
            return null;

        return doReadFloatArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws BinaryObjectException {
        return readDoubleArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws BinaryObjectException {
        if (checkFlag(DOUBLE_ARR) == Flag.NULL)
            return null;

        return doReadDoubleArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws BinaryObjectException {
        return readCharArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws BinaryObjectException {
        if (checkFlag(CHAR_ARR) == Flag.NULL)
            return null;

        return doReadCharArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws BinaryObjectException {
        return readBooleanArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws BinaryObjectException {
        if (checkFlag(BOOLEAN_ARR) == Flag.NULL)
            return null;

        return doReadBooleanArray();
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws BinaryObjectException {
        return readDecimalArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray() throws BinaryObjectException {
        if (checkFlag(DECIMAL_ARR) == Flag.NULL)
            return null;

        return doReadDecimalArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws BinaryObjectException {
        return readStringArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws BinaryObjectException {
        if (checkFlag(STRING_ARR) == Flag.NULL)
            return null;

        return doReadStringArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws BinaryObjectException {
        return readUuidArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws BinaryObjectException {
        if (checkFlag(UUID_ARR) == Flag.NULL)
            return null;

        return doReadUuidArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray(String fieldName) throws BinaryObjectException {
        return readDateArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp[] readTimestampArray(String fieldName) throws BinaryObjectException {
        return readTimestampArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray() throws BinaryObjectException {
        if (checkFlag(DATE_ARR) == Flag.NULL)
            return null;

        return doReadDateArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp[] readTimestampArray() throws BinaryObjectException {
        if (checkFlag(TIMESTAMP_ARR) == Flag.NULL)
            return null;

        return doReadTimestampArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws BinaryObjectException {
        return readObjectArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws BinaryObjectException {
        if (checkFlag(OBJ_ARR) == Flag.NULL)
            return null;

        return doReadObjectArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws BinaryObjectException {
        return readCollection(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws BinaryObjectException {
        if (checkFlag(COL) == Flag.NULL)
            return null;

        return (Collection<T>)doReadCollection(true, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName,
        Class<? extends Collection<T>> colCls) throws BinaryObjectException {
        return readCollection(fieldId(fieldName), colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(Class<? extends Collection<T>> colCls)
        throws BinaryObjectException {
        if (checkFlag(COL) == Flag.NULL)
            return null;

        return (Collection<T>)doReadCollection(true, colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws BinaryObjectException {
        return (Map<K, V>)readMap(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws BinaryObjectException {
        if (checkFlag(MAP) == Flag.NULL)
            return null;

        return (Map<K, V>)doReadMap(true, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws BinaryObjectException {
        return (Map<K, V>)readMap(fieldId(fieldName), mapCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(Class<? extends Map<K, V>> mapCls)
        throws BinaryObjectException {
        if (checkFlag(MAP) == Flag.NULL)
            return null;

        return (Map<K, V>)doReadMap(true, mapCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName)
        throws BinaryObjectException {
        return (T)readEnum(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum() throws BinaryObjectException {
        if (checkFlag(ENUM) == Flag.NULL)
            return null;

        Class cls = doReadClass();

        return (T)doReadEnum(cls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray(String fieldName)
        throws BinaryObjectException {
        return (T[])readEnumArray(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray() throws BinaryObjectException {
        if (checkFlag(ENUM_ARR) == Flag.NULL)
            return null;

        Class cls = doReadClass();

        return (T[])doReadEnumArray(cls);
    }

    /**
     * Ensure that type flag is either null or contains expected value.
     *
     * @param expFlag Expected value.
     * @return Flag.
     * @throws org.apache.ignite.binary.BinaryObjectException If flag is neither null, nor expected.
     */
    private Flag checkFlag(byte expFlag) {
        byte flag = in.readByte();

        if (flag == NULL)
            return Flag.NULL;
        else if (flag == HANDLE)
            return Flag.HANDLE;
        else if (flag != expFlag) {
            int pos = in.position() - 1;

            throw new BinaryObjectException("Unexpected flag value [pos=" + pos + ", expected=" + expFlag +
                ", actual=" + flag + ']');
        }

        return Flag.NORMAL;
    }

    /**
     * @param fieldName Field name.
     * @return {@code True} if field is set.
     */
    public boolean hasField(String fieldName) {
        return hasField(fieldId(fieldName));
    }

    /**
     * @param fieldId Field ID.
     * @return {@code True} if field is set.
     */
    private boolean hasField(int fieldId) {
        return fieldOffset(fieldId) != 0;
    }

    /** {@inheritDoc} */
    @Override public BinaryRawReader rawReader() {
        in.position(rawOff);

        return this;
    }

    /**
     * @return Unmarshalled value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable private Object unmarshal(boolean detach) throws BinaryObjectException {
        int start = in.position();

        byte flag = in.readByte();

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = start - in.readInt();

                BinaryObject handledPo = rCtx.getPortableByHandle(handle);

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

                rCtx.setPortableHandler(start, po);

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

        in.position(in.position() + strLen);

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @Nullable private Object doReadObject() throws BinaryObjectException {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx, in, in.position(), ldr, rCtx);

        return reader.deserialize();
    }

    /**
     * @return Deserialized object.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
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

                obj = rCtx.getObjectByHandle(handle);

                if (obj == null) {
                    int retPos = in.position();

                    in.position(handle);

                    obj = doReadObject();

                    in.position(retPos);
                }

                break;

            case OBJ:
                parseHeaderIfNeeded();

                assert typeId != UNREGISTERED_TYPE_ID;

                PortableUtils.checkProtocolVersion(in.readByte());

                boolean userType = PortableUtils.isUserType(this.readShort());

                // Skip typeId and hash code.
                in.position(in.position() + 8);

                desc = ctx.descriptorForTypeId(userType, typeId, ldr);

                int len = in.readInt();

                in.position(start + hdrLen);

                if (desc == null)
                    throw new BinaryInvalidTypeException("Unknown type ID: " + typeId);

                obj = desc.read(this);

                in.position(start + len);

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
        int hPos = in.position() - 1;

        int len = in.readInt();

        byte[] arr = in.readByteArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private short[] doReadShortArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        short[] arr = in.readShortArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private int[] doReadIntArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        int[] arr = in.readIntArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private long[] doReadLongArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        long[] arr = in.readLongArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private float[] doReadFloatArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        float[] arr = in.readFloatArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private double[] doReadDoubleArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        double[] arr = in.readDoubleArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private char[] doReadCharArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        char[] arr = in.readCharArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     */
    private boolean[] doReadBooleanArray() {
        int hPos = in.position() - 1;

        int len = in.readInt();

        boolean[] arr = in.readBooleanArray(len);

        setHandler(arr, hPos);

        return arr;
    }

    /**
     * @return Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private BigDecimal[] doReadDecimalArray() throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private String[] doReadStringArray() throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private UUID[] doReadUuidArray() throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private Date[] doReadDateArray() throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private Timestamp[] doReadTimestampArray() throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private Object[] doReadObjectArray(boolean deep) throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Collection<?> doReadCollection(boolean deep, @Nullable Class<? extends Collection> cls)
        throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Map<?, ?> doReadMap(boolean deep, @Nullable Class<? extends Map> cls)
        throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private Map.Entry<?, ?> doReadMapEntry(boolean deep) throws BinaryObjectException {
        int hPos = in.position() - 1;

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
     * @param cls Enum class.
     * @return Value.
     */
    private Enum<?> doReadEnum(Class<?> cls) throws BinaryObjectException {
        if (!cls.isEnum())
            throw new BinaryObjectException("Class does not represent enum type: " + cls.getName());

        int ord = in.readInt();

        return ord >= 0 ? (Enum<?>)GridEnumCache.get(cls)[ord] : null;
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
     * @param name Field name.
     * @return Field offset.
     */
    private int fieldId(String name) {
        assert name != null;

        parseHeaderIfNeeded();

        assert typeId != UNREGISTERED_TYPE_ID;

        if (idMapper == null)
            idMapper = ctx.userTypeIdMapper(typeId);

        return idMapper.fieldId(typeId, name);
    }

    /**
     * Create schema.
     *
     * @return Schema.
     */
    public PortableSchema createSchema() {
        parseHeaderIfNeeded();

        LinkedHashMap<Integer, Integer> fields = new LinkedHashMap<>();

        int searchPos = footerStart;
        int searchEnd = searchPos + footerLen;

        int idx = 0;

        while (searchPos < searchEnd) {
            int fieldId = in.readIntPositioned(searchPos);

            fields.put(fieldId, idx++);

            searchPos += 4 + offsetSize;
        }

        return new PortableSchema(fields);
    }

    /**
     * @param id Field ID.
     * @return Field offset.
     */
    private int fieldOffset(int id) {
        assert hdrLen != 0;

        if (footerLen == 0)
            return 0;

        int searchPos = footerStart;
        int searchTail = searchPos + footerLen;

        if (hasLowFieldsCount(footerLen)) {
            while (true) {
                if (searchPos >= searchTail)
                    return 0;

                int id0 = in.readIntPositioned(searchPos);

                if (id0 == id) {
                    int pos = start + PortableUtils.fieldOffsetRelative(in, searchPos + 4, offsetSize);

                    in.position(pos);

                    return pos;
                }

                searchPos += 4 + offsetSize;
            }
        }
        else {
            PortableSchema schema0 = schema;

            if (schema0 == null) {
                schema0 = ctx.schemaRegistry(typeId).schema(schemaId);

                if (schema0 == null) {
                    schema0 = createSchema();

                    ctx.schemaRegistry(typeId).addSchema(schemaId, schema0);
                }

                schema = schema0;
            }

            int order = schema.order(id);

            if (order != PortableSchema.ORDER_NOT_FOUND) {
                int offsetPos = footerStart + order * (4 + offsetSize) + 4;

                int pos = start + PortableUtils.fieldOffsetRelative(in, offsetPos, offsetSize);

                in.position(pos);

                return pos;
            }
            else
                return 0;
        }
    }

    /**
     * Check whether object has low amount of fields.
     *
     * @param footerLen Footer length.
     */
    private boolean hasLowFieldsCount(int footerLen) {
        assert hdrParsed;

        return footerLen < (FIELD_DESC_LEN << 4);
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
        /** Null. */
        NULL,

        /** Handle. */
        HANDLE,

        /** Regular. */
        NORMAL
    }
}
