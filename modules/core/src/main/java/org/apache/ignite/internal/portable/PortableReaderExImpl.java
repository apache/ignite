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
import org.apache.ignite.internal.portable.streams.PortableHeapInputStream;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.util.GridEnumCache;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableInvalidClassException;
import org.apache.ignite.portable.PortableObject;
import org.apache.ignite.portable.PortableRawReader;
import org.apache.ignite.portable.PortableReader;
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
import static org.apache.ignite.internal.portable.GridPortableMarshaller.RAW_DATA_OFF_POS;
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
public class PortableReaderExImpl implements PortableReader, PortableRawReaderEx, ObjectInput {
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
    private int len;

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

    /**
     * @param ctx Context.
     * @param arr Array.
     * @param start Start.
     * @param ldr Class loader.
     */
    public PortableReaderExImpl(PortableContext ctx, byte[] arr, int start, ClassLoader ldr) {
        this(ctx, new PortableHeapInputStream(arr), start, ldr, new PortableReaderContext());
    }

    /**
     * @param ctx Context.
     * @param in Input stream.
     * @param start Start.
     */
    PortableReaderExImpl(PortableContext ctx, PortableInputStream in, int start, ClassLoader ldr) {
        this(ctx, in, start, ldr, new PortableReaderContext());
    }

    /**
     * @param ctx Context.
     * @param in Input stream.
     * @param start Start.
     * @param rCtx Context.
     */
    PortableReaderExImpl(PortableContext ctx, PortableInputStream in, int start, ClassLoader ldr,
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
    private void parseHeaderIfNeeded(boolean skipObjByte) {
        if (hdrParsed)
            return;

        int retPos = in.position();

        in.position(start);

        byte hdr = in.readByte();

        if (hdr != GridPortableMarshaller.OBJ)
            throw new PortableException("Invalid header [pos=" + retPos + "expected=" + GridPortableMarshaller.OBJ +
                ", actual=" + hdr + ']');

        PortableUtils.checkProtocolVersion(in.readByte());

        in.position(in.position() + 2); // Skip flags.

        typeId = in.readInt();

        rawOff = in.readInt(start + RAW_DATA_OFF_POS);

        if (typeId == UNREGISTERED_TYPE_ID) {
            // Skip to the class name position.
            in.position(in.position() + GridPortableMarshaller.DFLT_HDR_LEN - 8);

            int off = in.position();

            Class cls = doReadClass(true, typeId);

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
     * @throws PortableException In case of error.
     */
    @Nullable Object unmarshal() throws PortableException {
        return unmarshal(true);
    }

    /**
     * @param fieldName Field name.
     * @return Unmarshalled value.
     * @throws PortableException In case of error.
     */
    @Nullable Object unmarshal(String fieldName) throws PortableException {
        return hasField(fieldName) ? unmarshal(false) : null;
    }

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws PortableException In case of error.
     */
    public Object unmarshal(int offset) throws PortableException {
        in.position(offset);

        return in.position() >= 0 ? unmarshal(false) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Byte readByte(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(BYTE, false) == Flag.NULL)
                return null;

            return in.readByte();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Short readShort(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(SHORT, false) == Flag.NULL)
                return null;

            return in.readShort();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Integer readInt(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(INT, false) == Flag.NULL)
                return null;

            return in.readInt();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Long readLong(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(LONG, false) == Flag.NULL)
                return null;

            return in.readLong();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Float readFloat(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(FLOAT, false) == Flag.NULL)
                return null;

            return in.readFloat();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Double readDouble(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(DOUBLE, false) == Flag.NULL)
                return null;

            return in.readDouble();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Character readChar(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(CHAR, false) == Flag.NULL)
                return null;

            return in.readChar();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Boolean readBoolean(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(BOOLEAN, false) == Flag.NULL)
                return null;

            return in.readBoolean();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable BigDecimal readDecimal(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(DECIMAL, false) == Flag.NULL)
                return null;

            return doReadDecimal();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable String readString(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(STRING, false) == Flag.NULL)
                return null;

            return doReadString();
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable UUID readUuid(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(UUID, false) == Flag.NULL)
                return null;

            return doReadUuid(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Date readDate(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(DATE, false) == Flag.NULL)
                return null;

            return doReadDate(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Timestamp readTimestamp(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(TIMESTAMP, false) == Flag.NULL)
                return null;

            return doReadTimestamp(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Object readObject(int fieldId) throws PortableException {
        return hasField(fieldId) ? doReadObject(false) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable byte[] readByteArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(BYTE_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable short[] readShortArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(SHORT_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable int[] readIntArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(INT_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable long[] readLongArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(LONG_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable float[] readFloatArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(FLOAT_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable double[] readDoubleArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(DOUBLE_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable char[] readCharArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(CHAR_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable boolean[] readBooleanArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(BOOLEAN_ARR, false);

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
     * @throws PortableException In case of error.
     */
    @Nullable BigDecimal[] readDecimalArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(DECIMAL_ARR, false);

            if (flag == Flag.NORMAL)
                return doReadDecimalArray(false);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable String[] readStringArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(STRING_ARR, false);

            if (flag == Flag.NORMAL)
                return doReadStringArray(false);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable UUID[] readUuidArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(UUID_ARR, false);

            if (flag == Flag.NORMAL)
                return doReadUuidArray(false);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Date[] readDateArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(DATE_ARR, false);

            if (flag == Flag.NORMAL)
                return doReadDateArray(false);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Timestamp[] readTimestampArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(TIMESTAMP_ARR, false);

            if (flag == Flag.NORMAL)
                return doReadTimestampArray(false);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Object[] readObjectArray(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(OBJ_ARR, false);

            if (flag == Flag.NORMAL)
                return doReadObjectArray(false, true);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Collection class.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable <T> Collection<T> readCollection(int fieldId, @Nullable Class<? extends Collection> cls)
        throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(COL, false);

            if (flag == Flag.NORMAL)
                return (Collection<T>)doReadCollection(false, true, cls);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Map class.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Map<?, ?> readMap(int fieldId, @Nullable Class<? extends Map> cls)
        throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(MAP, false);

            if (flag == Flag.NORMAL)
                return doReadMap(false, true, cls);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException On case of error.
     */
    @Nullable Map.Entry<?, ?> readMapEntry(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(MAP_ENTRY, false);

            if (flag == Flag.NORMAL)
                return doReadMapEntry(false, true);
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Portable object.
     * @throws PortableException In case of error.
     */
    @Nullable PortableObject readPortableObject(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(PORTABLE_OBJ, false) == Flag.NULL)
                return null;

            return new PortableObjectImpl(ctx, doReadByteArray(), in.readInt());
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @param cls Class.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Enum<?> readEnum(int fieldId, @Nullable Class<?> cls) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(ENUM, false) == Flag.NULL)
                return null;

            // Revisit: why have we started writing Class for enums in their serialized form?
            if (cls == null)
                cls = doReadClass(false);
            else
                doReadClass(false);

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
     * @throws PortableException In case of error.
     */
    @Nullable Object[] readEnumArray(int fieldId, @Nullable Class<?> cls) throws PortableException {
        if (hasField(fieldId)) {
            Flag flag = checkFlag(ENUM_ARR, false);

            if (flag == Flag.NORMAL) {
                if (cls == null)
                    cls = doReadClass(false);
                else
                    doReadClass(false);

                return doReadEnumArray(false, cls);
            }
            else if (flag == Flag.HANDLE)
                return readHandleField();
        }

        return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Field class.
     * @throws PortableException In case of error.
     */
    @Nullable Class<?> readClass(int fieldId) throws PortableException {
        if (hasField(fieldId)) {
            if (checkFlag(CLASS, false) == Flag.NULL)
                return null;

            return doReadClass(false);
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

            obj = doReadObject(false);
        }

        return (T)obj;
    }
    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws PortableException {
        Byte val = readByte(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws PortableException {
        return in.readByte();
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws PortableException {
        Short val = readShort(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws PortableException {
        return in.readShort();
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws PortableException {
        Integer val = readInt(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws PortableException {
        return in.readInt();
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws PortableException {
        Long val = readLong(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws PortableException {
        return in.readLong();
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws PortableException {
        Float val = readFloat(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws PortableException {
        return in.readFloat();
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws PortableException {
        Double val = readDouble(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws PortableException {
        return in.readDouble();
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws PortableException {
        Character val = readChar(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws PortableException {
        return in.readChar();
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws PortableException {
        Boolean val = readBoolean(fieldId(fieldName));

        return val != null ? val : false;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws PortableException {
        return in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal(String fieldName) throws PortableException {
        return readDecimal(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal() throws PortableException {
        if (checkFlag(DECIMAL, true) == Flag.NULL)
            return null;

        return doReadDecimal();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws PortableException {
        return readString(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws PortableException {
        if (checkFlag(STRING, true) == Flag.NULL)
            return null;

        return doReadString();
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws PortableException {
        return readUuid(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws PortableException {
        if (checkFlag(UUID, true) == Flag.NULL)
            return null;

        return doReadUuid(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate(String fieldName) throws PortableException {
        return readDate(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate() throws PortableException {
        if (checkFlag(DATE, true) == Flag.NULL)
            return null;

        return doReadDate(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp(String fieldName) throws PortableException {
        return readTimestamp(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp() throws PortableException {
        if (checkFlag(TIMESTAMP, true) == Flag.NULL)
            return null;

        return doReadTimestamp(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T readObject(String fieldName) throws PortableException {
        return (T)readObject(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public Object readObject() throws PortableException {
        return doReadObject(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object readObjectDetached() throws PortableException {
        return unmarshal(true, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws PortableException {
        return readByteArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws PortableException {
        if (checkFlag(BYTE_ARR, true) == Flag.NULL)
            return null;

        return doReadByteArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws PortableException {
        return readShortArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws PortableException {
        if (checkFlag(SHORT_ARR, true) == Flag.NULL)
            return null;

        return doReadShortArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws PortableException {
        return readIntArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws PortableException {
        if (checkFlag(INT_ARR, true) == Flag.NULL)
            return null;

        return doReadIntArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws PortableException {
        return readLongArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws PortableException {
        if (checkFlag(LONG_ARR, true) == Flag.NULL)
            return null;

        return doReadLongArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws PortableException {
        return readFloatArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws PortableException {
        if (checkFlag(FLOAT_ARR, true) == Flag.NULL)
            return null;

        return doReadFloatArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws PortableException {
        return readDoubleArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws PortableException {
        if (checkFlag(DOUBLE_ARR, true) == Flag.NULL)
            return null;

        return doReadDoubleArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws PortableException {
        return readCharArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws PortableException {
        if (checkFlag(CHAR_ARR, true) == Flag.NULL)
            return null;

        return doReadCharArray();
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws PortableException {
        return readBooleanArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws PortableException {
        if (checkFlag(BOOLEAN_ARR, true) == Flag.NULL)
            return null;

        return doReadBooleanArray();
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws PortableException {
        return readDecimalArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray() throws PortableException {
        if (checkFlag(DECIMAL_ARR, true) == Flag.NULL)
            return null;

        return doReadDecimalArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws PortableException {
        return readStringArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws PortableException {
        if (checkFlag(STRING_ARR, true) == Flag.NULL)
            return null;

        return doReadStringArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws PortableException {
        return readUuidArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws PortableException {
        if (checkFlag(UUID_ARR, true) == Flag.NULL)
            return null;

        return doReadUuidArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray(String fieldName) throws PortableException {
        return readDateArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp[] readTimestampArray(String fieldName) throws PortableException {
        return readTimestampArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray() throws PortableException {
        if (checkFlag(DATE_ARR, true) == Flag.NULL)
            return null;

        return doReadDateArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp[] readTimestampArray() throws PortableException {
        if (checkFlag(TIMESTAMP_ARR, true) == Flag.NULL)
            return null;

        return doReadTimestampArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws PortableException {
        return readObjectArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws PortableException {
        if (checkFlag(OBJ_ARR, true) == Flag.NULL)
            return null;

        return doReadObjectArray(true, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws PortableException {
        return readCollection(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws PortableException {
        if (checkFlag(COL, true) == Flag.NULL)
            return null;

        return (Collection<T>)doReadCollection(true, true, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName,
        Class<? extends Collection<T>> colCls) throws PortableException {
        return readCollection(fieldId(fieldName), colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(Class<? extends Collection<T>> colCls)
        throws PortableException {
        if (checkFlag(COL, true) == Flag.NULL)
            return null;

        return (Collection<T>)doReadCollection(true, true, colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws PortableException {
        return (Map<K, V>)readMap(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws PortableException {
        if (checkFlag(MAP, true) == Flag.NULL)
            return null;

        return (Map<K, V>)doReadMap(true, true, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws PortableException {
        return (Map<K, V>)readMap(fieldId(fieldName), mapCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(Class<? extends Map<K, V>> mapCls)
        throws PortableException {
        if (checkFlag(MAP, true) == Flag.NULL)
            return null;

        return (Map<K, V>)doReadMap(true, true, mapCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName)
        throws PortableException {
        return (T)readEnum(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum() throws PortableException {
        if (checkFlag(ENUM, true) == Flag.NULL)
            return null;

        Class cls = doReadClass(true);

        return (T)doReadEnum(true, cls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray(String fieldName)
        throws PortableException {
        return (T[])readEnumArray(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T[] readEnumArray() throws PortableException {
        if (checkFlag(ENUM_ARR, true) == Flag.NULL)
            return null;

        Class cls = doReadClass(true);

        return (T[])doReadEnumArray(true, cls);
    }

    /**
     * Ensure that type flag is either null or contains expected value.
     *
     * @param expFlag Expected value.
     * @param raw Raw flag.
     * @return Flag.
     * @throws PortableException If flag is neither null, nor expected.
     */
    private Flag checkFlag(byte expFlag, boolean raw) {
        byte flag = in.readByte();

        if (flag == NULL)
            return Flag.NULL;
        else if (flag == HANDLE)
            return Flag.HANDLE;
        else if (flag != expFlag) {
            int pos = in.position() - 1;

            throw new PortableException("Unexpected flag value [pos=" + pos + ", expected=" + expFlag +
                ", actual=" + flag + ']');
        }

        return Flag.NORMAL;
    }

    /**
     * @param fieldName Field name.
     * @return {@code true} if field is set.
     */
    public boolean hasField(String fieldName) {
        return hasField(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override public PortableRawReader rawReader() {
        in.position(start + rawOff);

        return this;
    }

    /**
     * @param raw Raw flag.
     * @return Unmarshalled value.
     * @throws PortableException In case of error.
     */
    @Nullable private Object unmarshal(boolean raw) throws PortableException {
        return unmarshal(raw, false);
    }

    /**
     * @param raw Raw flag.
     * @return Unmarshalled value.
     * @throws PortableException In case of error.
     */
    @Nullable private Object unmarshal(boolean raw, boolean detach) throws PortableException {
        int start = in.position();

        byte flag = in.readByte();

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = start - in.readInt();

                PortableObject handledPo = rCtx.getPortableByHandle(handle);

                if (handledPo != null)
                    return handledPo;

                in.position(handle);

                return unmarshal(false);

            case OBJ:
                PortableUtils.checkProtocolVersion(in.readByte());

                PortableObjectEx po;

                if (detach) {
                    in.position(start + GridPortableMarshaller.TOTAL_LEN_POS);

                    int len = in.readInt();

                    in.position(start);

                    po = new PortableObjectImpl(ctx, in.readByteArray(len), 0);
                }
                else
                    po = in.offheapPointer() > 0
                        ? new PortableObjectOffheapImpl(ctx, in.offheapPointer(), start,
                        in.remaining() + in.position())
                        : new PortableObjectImpl(ctx, in.array(), start);

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
                return doReadUuid(raw);

            case DATE:
                return doReadDate(raw);

            case TIMESTAMP:
                return doReadTimestamp(raw);

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
                return doReadDecimalArray(raw);

            case STRING_ARR:
                return doReadStringArray(raw);

            case UUID_ARR:
                return doReadUuidArray(raw);

            case DATE_ARR:
                return doReadDateArray(raw);

            case TIMESTAMP_ARR:
                return doReadTimestampArray(raw);

            case OBJ_ARR:
                return doReadObjectArray(raw, false);

            case COL:
                return doReadCollection(raw, false, null);

            case MAP:
                return doReadMap(raw, false, null);

            case MAP_ENTRY:
                return doReadMapEntry(raw, false);

            case PORTABLE_OBJ:
                return doReadPortableObject(raw);

            case ENUM:
                return doReadEnum(raw, doReadClass(raw));

            case ENUM_ARR:
                return doReadEnumArray(raw, doReadClass(raw));

            case CLASS:
                return in.readInt();

            case OPTM_MARSH:
                int len = in.readInt();

                ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

                Object obj;

                try {
                    obj = ctx.optimizedMarsh().unmarshal(input, null);
                }
                catch (IgniteCheckedException e) {
                    throw new PortableException("Failed to unmarshal object with optmMarsh marshaller", e);
                }

                return obj;

            default:
                throw new PortableException("Invalid flag value: " + flag);
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
        if (in.readBoolean()) {
            if (!in.hasArray())
                return new String(doReadByteArray(), UTF_8);

            int strLen = in.readInt();
            int strOff = in.position();

            String res = new String(in.array(), strOff, strLen, UTF_8);

            in.position(in.position() + strLen);

            // TODO: Opto.
            //in.position(in.position() + strLen);

            return res;
        }
        else
            return String.valueOf(doReadCharArray());
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private UUID doReadUuid(boolean raw) {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Date doReadDate(boolean raw) {
        long time = in.readLong();

        return new Date(time);
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Timestamp doReadTimestamp(boolean raw) {
        long time = in.readLong();
        int nanos = in.readInt();

        Timestamp ts = new Timestamp(time);

        ts.setNanos(ts.getNanos() + nanos);

        return ts;
    }

    /**
     * @param raw Raw flag.
     * @return Object.
     * @throws PortableException In case of error.
     */
    @Nullable private Object doReadObject(boolean raw) throws PortableException {
        PortableReaderExImpl reader = new PortableReaderExImpl(ctx, in, in.position(), ldr, rCtx);

        return reader.deserialize();
    }

    /**
     * @return Deserialized object.
     * @throws PortableException If failed.
     */
    @Nullable Object deserialize() throws PortableException {
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

                    obj = doReadObject(false);

                    in.position(retPos);
                }

                break;

            case OBJ:
                parseHeaderIfNeeded(true);

                assert typeId != UNREGISTERED_TYPE_ID;

                PortableUtils.checkProtocolVersion(in.readByte());

                boolean userType = PortableUtils.isUserType(PortableUtils.readFlags(this));

                // Skip typeId and hash code.
                in.position(in.position() + 8);

                desc = ctx.descriptorForTypeId(userType, typeId, ldr);

                len = in.readInt();

                in.position(start + hdrLen);

                if (desc == null)
                    throw new PortableInvalidClassException("Unknown type ID: " + typeId);

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
                obj = doReadUuid(true);

                break;

            case DATE:
                obj = doReadDate(true);

                break;

            case TIMESTAMP:
                obj = doReadTimestamp(true);

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
                obj = doReadDecimalArray(true);

                break;

            case STRING_ARR:
                obj = doReadStringArray(true);

                break;

            case UUID_ARR:
                obj = doReadUuidArray(true);

                break;

            case DATE_ARR:
                obj = doReadDateArray(true);

                break;

            case TIMESTAMP_ARR:
                obj = doReadTimestampArray(true);

                break;

            case OBJ_ARR:
                obj = doReadObjectArray(true, true);

                break;

            case COL:
                obj = doReadCollection(true, true, null);

                break;

            case MAP:
                obj = doReadMap(true, true, null);

                break;

            case MAP_ENTRY:
                obj = doReadMapEntry(true, true);

                break;

            case PORTABLE_OBJ:
                obj = doReadPortableObject(true);

                ((PortableObjectImpl)obj).context(ctx);

                if (!GridPortableMarshaller.KEEP_PORTABLES.get())
                    obj = ((PortableObject)obj).deserialize();

                break;

            case ENUM:
                obj = doReadEnum(true, doReadClass(true));

                break;

            case ENUM_ARR:
                obj = doReadEnumArray(true, doReadClass(true));

                break;

            case CLASS:
                obj = doReadClass(true);

                break;

            case OPTM_MARSH:
                int len = in.readInt();

                ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

                try {
                    obj = ctx.optimizedMarsh().unmarshal(input, null);
                }
                catch (IgniteCheckedException e) {
                    throw new PortableException("Failed to unmarshal object with optimized marshaller", e);
                }

                in.position(in.position() + len);

                break;

            default:
                throw new PortableException("Invalid flag value: " + flag);
        }

        if (len == 0)
            len = in.position() - start;

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
     * @param raw Raw flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private BigDecimal[] doReadDecimalArray(boolean raw) throws PortableException {
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
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadDecimal();
            }
        }

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private String[] doReadStringArray(boolean raw) throws PortableException {
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
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadString();
            }
        }

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private UUID[] doReadUuidArray(boolean raw) throws PortableException {
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
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadUuid(raw);
            }
        }

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private Date[] doReadDateArray(boolean raw) throws PortableException {
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
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadDate(raw);
            }
        }

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private Timestamp[] doReadTimestampArray(boolean raw) throws PortableException {
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
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadTimestamp(raw);
            }
        }

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @param deep Deep flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private Object[] doReadObjectArray(boolean raw, boolean deep) throws PortableException {
        int hPos = in.position() - 1;

        Class compType = doReadClass(raw);

        int len = in.readInt();

        Object[] arr = deep ? (Object[])Array.newInstance(compType, len) : new Object[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++)
            arr[i] = deep ? doReadObject(raw) : unmarshal(raw);

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @param deep Deep flag.
     * @param cls Collection class.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Collection<?> doReadCollection(boolean raw, boolean deep, @Nullable Class<? extends Collection> cls)
        throws PortableException {
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
                throw new PortableException("Collection class doesn't have public default constructor: " +
                    cls.getName());
            }
            catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new PortableException("Failed to instantiate collection: " + cls.getName(), e);
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
                    throw new PortableException("Invalid collection type: " + colType);
            }
        }

        setHandler(col, hPos);

        for (int i = 0; i < size; i++)
            col.add(deep ? doReadObject(raw) : unmarshal(raw));

        return col;
    }

    /**
     * @param raw Raw flag.
     * @param deep Deep flag.
     * @param cls Map class.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @SuppressWarnings("unchecked")
    private Map<?, ?> doReadMap(boolean raw, boolean deep, @Nullable Class<? extends Map> cls)
        throws PortableException {
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
                throw new PortableException("Map class doesn't have public default constructor: " +
                    cls.getName());
            }
            catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new PortableException("Failed to instantiate map: " + cls.getName(), e);
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
                    throw new PortableException("Invalid map type: " + mapType);
            }
        }

        setHandler(map, hPos);

        for (int i = 0; i < size; i++)
            map.put(deep ? doReadObject(raw) : unmarshal(raw), deep ? doReadObject(raw) : unmarshal(raw));

        return map;
    }

    /**
     * @param raw Raw flag.
     * @param deep Deep flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private Map.Entry<?, ?> doReadMapEntry(boolean raw, boolean deep) throws PortableException {
        int hPos = in.position() - 1;

        Object val1 = deep ? doReadObject(raw) : unmarshal(raw);
        Object val2 = deep ? doReadObject(raw) : unmarshal(raw);

        GridMapEntry entry = new GridMapEntry<>(val1, val2);

        setHandler(entry, hPos);

        return entry;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private PortableObject doReadPortableObject(boolean raw) {
        if (in.offheapPointer() > 0) {
            int len = in.readInt();

            int pos = in.position();

            in.position(in.position() + len);

            int start = in.readInt();

            return new PortableObjectOffheapImpl(ctx, in.offheapPointer() + pos, start, len);
        }
        else {
            byte[] arr = doReadByteArray();
            int start = in.readInt();

            return new PortableObjectImpl(ctx, arr, start);
        }
    }

    /**
     * @param raw Raw flag.
     * @param cls Enum class.
     * @return Value.
     */
    private Enum<?> doReadEnum(boolean raw, Class<?> cls) throws PortableException {
        if (!cls.isEnum())
            throw new PortableException("Class does not represent enum type: " + cls.getName());

        int ord = in.readInt();

        return ord >= 0 ? (Enum<?>)GridEnumCache.get(cls)[ord] : null;
    }

    /**
     * @param raw Raw flag.
     * @param cls Enum class.
     * @return Value.
     */
    private Object[] doReadEnumArray(boolean raw, Class<?> cls) throws PortableException {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(cls, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else
                arr[i] = doReadEnum(raw, doReadClass(raw));
        }

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Class doReadClass(boolean raw) throws PortableException {
        return doReadClass(raw, in.readInt());
    }

    /**
     * @param raw Raw flag.
     * @param typeId Type id.
     * @return Value.
     */
    private Class doReadClass(boolean raw, int typeId) throws PortableException {
        Class cls;

        if (typeId == OBJECT_TYPE_ID)
            return Object.class;

        if (typeId != UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr).describedClass();
        else {
            byte flag = in.readByte();

            if (flag != STRING)
                throw new PortableException("No class definition for typeId: " + typeId);

            String clsName = doReadString();

            try {
                cls = U.forName(clsName, ldr);
            }
            catch (ClassNotFoundException e) {
                throw new PortableInvalidClassException("Failed to load the class: " + clsName, e);
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

        parseHeaderIfNeeded(false);

        assert typeId != UNREGISTERED_TYPE_ID;

        return ctx.fieldId(typeId, name);
    }

    /**
     * @param id Field ID.
     * @return Field offset.
     */
    private boolean hasField(int id) {
        assert hdrLen != 0;

        int searchHead = start + hdrLen;
        int searchTail = start + in.readInt(start + RAW_DATA_OFF_POS);

//        int searchPos = in.position();
//
//        while (searchPos < searchTail) {
//            int id0 = in.readInt(searchPos);
//
//            if (id0 == id)
//                return searchPos + 8;
//
//            int len = in.readInt(searchPos + 4);
//
//            searchPos += (8 + len);
//        }
//
//        if (in.position() != searchHead) {
//            searchPos = searchHead;
//
//            while (searchPos < in.position()) {
//                int id0 = in.readInt(searchPos);
//
//                if (id0 == id)
//                    return searchPos + 8;
//
//                int len = in.readInt(searchPos + 4);
//
//                searchPos += (8 + len);
//            }
//        }
//
//        return -1;

        while (true) {
            if (searchHead >= searchTail)
                return false;

            int id0 = in.readInt(searchHead);

            if (id0 == id) {
                in.position(searchHead + 8);

                return true;
            }

            int len = in.readInt(searchHead + 4);

            searchHead += (8 + len);
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
    @NotNull @Override public String readUTF() throws IOException {
        return readString();
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    /** {@inheritDoc} */
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
        return skipBytes((int)n);
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
