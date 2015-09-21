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
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.portable.streams.PortableHeapInputStream;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.util.GridEnumCache;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.portable.api.PortableException;
import org.apache.ignite.internal.portable.api.PortableInvalidClassException;
import org.apache.ignite.internal.portable.api.PortableObject;
import org.apache.ignite.internal.portable.api.PortableRawReader;
import org.apache.ignite.internal.portable.api.PortableReader;
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
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLS_NAME_POS;
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
    private int off;

    /** */
    private int rawOff;

    /** */
    private int len;

    /** */
    private PortableClassDescriptor desc;

    /** */
    private int hdrLen;

    /** */
    private int clsNameLen;

    /** */
    private Integer typeId;

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

        off = start;
        rawOff = start;
    }

    /**
     * Preloads typeId from the input array.
     */
    private void readObjectTypeId(boolean skipObjByte) {
        int pos = rawOff;

        if (!skipObjByte)
            // skip obj type byte
            rawOff++;

        // skip user flag
        rawOff += 1;

        typeId = doReadInt(true);

        if (typeId == UNREGISTERED_TYPE_ID) {
            // skip hash code, length and raw offset
            rawOff += 12;

            int off = rawOff;

            Class cls = doReadClass(true, typeId);

            // registers class by typeId, at least locally if the cache is not ready yet.
            PortableClassDescriptor desc = ctx.descriptorForClass(cls);

            typeId = desc.typeId();

            clsNameLen = rawOff - off;

            hdrLen = CLS_NAME_POS + clsNameLen;
        }
        else
            hdrLen = DFLT_HDR_LEN;

        in.position(rawOff = pos);
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
        off = fieldOffset(fieldId(fieldName));

        return off >= 0 ? unmarshal(false) : null;
    }

    /**
     * @param offset Offset in the array.
     * @return Unmarshalled value.
     * @throws PortableException In case of error.
     */
    public Object unmarshal(int offset) throws PortableException {
        off = offset;

        return off >= 0 ? unmarshal(false) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Byte readByte(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != BYTE)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadByte(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != SHORT)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadShort(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != INT)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadInt(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != LONG)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadLong(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != FLOAT)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadFloat(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != DOUBLE)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadDouble(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != CHAR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadChar(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != BOOLEAN)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadBoolean(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != DECIMAL)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadDecimal(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != STRING)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadString(false);
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != UUID)
                throw new PortableException("Invalid flag value: " + flag);

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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != DATE)
                throw new PortableException("Invalid flag value: " + flag);

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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != DATE)
                throw new PortableException("Invalid flag value: " + flag);

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
        off = fieldOffset(fieldId);

        return off >= 0 ? doReadObject(false) : null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable byte[] readByteArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != BYTE_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadByteArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable short[] readShortArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != SHORT_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadShortArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable int[] readIntArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != INT_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadIntArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable long[] readLongArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != LONG_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadLongArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable float[] readFloatArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != FLOAT_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadFloatArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable double[] readDoubleArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != DOUBLE_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadDoubleArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable char[] readCharArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != CHAR_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadCharArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable boolean[] readBooleanArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != BOOLEAN_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadBooleanArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable BigDecimal[] readDecimalArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != DECIMAL_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadDecimalArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable String[] readStringArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != STRING_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadStringArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable UUID[] readUuidArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != UUID_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadUuidArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Date[] readDateArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != DATE_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadDateArray(false);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable Object[] readObjectArray(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != OBJ_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadObjectArray(false, true);
        }
        else
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != COL)
                throw new PortableException("Invalid flag value: " + flag);

            return (Collection<T>)doReadCollection(false, true, cls);
        }
        else
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != MAP)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadMap(false, true, cls);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Value.
     * @throws PortableException On case of error.
     */
    @Nullable Map.Entry<?, ?> readMapEntry(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag == HANDLE)
                return readHandleField();

            if (flag != MAP_ENTRY)
                throw new PortableException("Invalid flag value: " + flag);

            return doReadMapEntry(false, true);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Portable object.
     * @throws PortableException In case of error.
     */
    @Nullable PortableObject readPortableObject(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != PORTABLE_OBJ)
                throw new PortableException("Invalid flag value: " + flag);

            return new PortableObjectImpl(ctx, doReadByteArray(false), doReadInt(false));
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != ENUM)
                throw new PortableException("Invalid flag value: " + flag);

            // Revisit: why have we started writing Class for enums in their serialized form?
            if (cls == null)
                cls = doReadClass(false);
            else
                doReadClass(false);

            Object[] vals = GridEnumCache.get(cls);

            return (Enum<?>)vals[doReadInt(false)];
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
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != ENUM_ARR)
                throw new PortableException("Invalid flag value: " + flag);

            // Revisit: why have we started writing Class for enums in their serialized form?
            if (cls == null)
                cls = doReadClass(false);
            else
                doReadClass(false);

            return doReadEnumArray(false, cls);
        }
        else
            return null;
    }

    /**
     * @param fieldId Field ID.
     * @return Field class.
     * @throws PortableException In case of error.
     */
    @Nullable Class<?> readClass(int fieldId) throws PortableException {
        off = fieldOffset(fieldId);

        if (off >= 0) {
            byte flag = doReadByte(false);

            if (flag == NULL)
                return null;

            if (flag != CLASS)
                throw new PortableException("Invalid flag type: [flag=" + flag + ']');

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
        int handle = (off - 1) - doReadInt(false);

        Object obj = rCtx.getObjectByHandle(handle);

        if (obj == null) {
            off = handle;

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
        return doReadByte(true);
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws PortableException {
        Short val = readShort(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws PortableException {
        return doReadShort(true);
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws PortableException {
        Integer val = readInt(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws PortableException {
        return doReadInt(true);
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws PortableException {
        Long val = readLong(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws PortableException {
        return doReadLong(true);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws PortableException {
        Float val = readFloat(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws PortableException {
        return doReadFloat(true);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws PortableException {
        Double val = readDouble(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws PortableException {
        return doReadDouble(true);
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws PortableException {
        Character val = readChar(fieldId(fieldName));

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws PortableException {
        return doReadChar(true);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws PortableException {
        Boolean val = readBoolean(fieldId(fieldName));

        return val != null ? val : false;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws PortableException {
        return doReadBoolean(true);
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal(String fieldName) throws PortableException {
        return readDecimal(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal readDecimal() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != DECIMAL)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadDecimal(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws PortableException {
        return readString(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != STRING)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadString(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws PortableException {
        return readUuid(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != UUID)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadUuid(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate(String fieldName) throws PortableException {
        return readDate(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date readDate() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != DATE)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadDate(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp(String fieldName) throws PortableException {
        return readTimestamp(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Timestamp readTimestamp() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != DATE)
            throw new PortableException("Invalid flag value: " + flag);

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
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != BYTE_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadByteArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws PortableException {
        return readShortArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != SHORT_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadShortArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws PortableException {
        return readIntArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != INT_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadIntArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws PortableException {
        return readLongArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != LONG_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadLongArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws PortableException {
        return readFloatArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != FLOAT_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadFloatArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws PortableException {
        return readDoubleArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != DOUBLE_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadDoubleArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws PortableException {
        return readCharArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != CHAR_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadCharArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws PortableException {
        return readBooleanArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != BOOLEAN_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadBooleanArray(true);
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws PortableException {
        return readDecimalArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Override @Nullable public BigDecimal[] readDecimalArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != DECIMAL_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadDecimalArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws PortableException {
        return readStringArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != STRING_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadStringArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws PortableException {
        return readUuidArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != UUID_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadUuidArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray(String fieldName) throws PortableException {
        return readDateArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Date[] readDateArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != DATE_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadDateArray(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws PortableException {
        return readObjectArray(fieldId(fieldName));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != OBJ_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        return doReadObjectArray(true, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws PortableException {
        return readCollection(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != COL)
            throw new PortableException("Invalid flag value: " + flag);

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
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != COL)
            throw new PortableException("Invalid flag value: " + flag);

        return (Collection<T>)doReadCollection(true, true, colCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws PortableException {
        return (Map<K, V>)readMap(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != MAP)
            throw new PortableException("Invalid flag value: " + flag);

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
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != MAP)
            throw new PortableException("Invalid flag value: " + flag);

        return (Map<K, V>)doReadMap(true, true, mapCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum(String fieldName)
        throws PortableException {
        return (T)readEnum(fieldId(fieldName), null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Enum<?>> T readEnum() throws PortableException {
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != ENUM)
            throw new PortableException("Invalid flag value: " + flag);

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
        byte flag = doReadByte(true);

        if (flag == NULL)
            return null;

        if (flag != ENUM_ARR)
            throw new PortableException("Invalid flag value: " + flag);

        Class cls = doReadClass(true);

        return (T[])doReadEnumArray(true, cls);
    }

    /**
     * @param fieldName Field name.
     * @return {@code true} if field is set.
     */
    public boolean hasField(String fieldName) {
        return fieldOffset(fieldId(fieldName)) != -1;
    }

    /** {@inheritDoc} */
    @Override public PortableRawReader rawReader() {
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
        int start = raw ? rawOff : off;

        byte flag = doReadByte(raw);

        switch (flag) {
            case NULL:
                return null;

            case HANDLE:
                int handle = start - doReadInt(raw);

                PortableObject handledPo = rCtx.getPortableByHandle(handle);

                if (handledPo != null)
                    return handledPo;

                off = handle;

                return unmarshal(false);

            case OBJ:
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

                if (raw)
                    rawOff = start + po.length();
                else
                    off = start + po.length();

                return po;

            case BYTE:
                return doReadByte(raw);

            case SHORT:
                return doReadShort(raw);

            case INT:
                return doReadInt(raw);

            case LONG:
                return doReadLong(raw);

            case FLOAT:
                return doReadFloat(raw);

            case DOUBLE:
                return doReadDouble(raw);

            case CHAR:
                return doReadChar(raw);

            case BOOLEAN:
                return doReadBoolean(raw);

            case DECIMAL:
                return doReadDecimal(raw);

            case STRING:
                return doReadString(raw);

            case UUID:
                return doReadUuid(raw);

            case DATE:
                return isUseTimestamp() ? doReadTimestamp(raw) : doReadDate(raw);

            case BYTE_ARR:
                return doReadByteArray(raw);

            case SHORT_ARR:
                return doReadShortArray(raw);

            case INT_ARR:
                return doReadIntArray(raw);

            case LONG_ARR:
                return doReadLongArray(raw);

            case FLOAT_ARR:
                return doReadFloatArray(raw);

            case DOUBLE_ARR:
                return doReadDoubleArray(raw);

            case CHAR_ARR:
                return doReadCharArray(raw);

            case BOOLEAN_ARR:
                return doReadBooleanArray(raw);

            case DECIMAL_ARR:
                return doReadDecimalArray(raw);

            case STRING_ARR:
                return doReadStringArray(raw);

            case UUID_ARR:
                return doReadUuidArray(raw);

            case DATE_ARR:
                return doReadDateArray(raw);

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
                return doReadInt(raw);

            case OPTM_MARSH:
                int len = doReadInt(true);

                ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

                Object obj;

                try {
                    obj = ctx.optimizedMarsh().unmarshal(input, null);
                }
                catch (IgniteCheckedException e) {
                    throw new PortableException("Failed to unmarshal object with optmMarsh marshaller", e);
                }

                if (raw)
                    rawOff += len;
                else
                    off += len;

                return obj;

            default:
                throw new PortableException("Invalid flag value: " + flag);
        }
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private byte doReadByte(boolean raw) {
        in.position(raw ? rawOff++ : off++);

        return in.readByte();
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private short doReadShort(boolean raw) {
        in.position(raw ? rawOff : off);

        short val = in.readShort();

        if (raw)
            rawOff += 2;
        else
            off += 2;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private int doReadInt(boolean raw) {
        in.position(raw ? rawOff : off);

        int val = in.readInt();

        if (raw)
            rawOff += 4;
        else
            off += 4;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private long doReadLong(boolean raw) {
        in.position(raw ? rawOff : off);

        long val = in.readLong();

        if (raw)
            rawOff += 8;
        else
            off += 8;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private float doReadFloat(boolean raw) {
        in.position(raw ? rawOff : off);

        float val = in.readFloat();

        if (raw)
            rawOff += 4;
        else
            off += 4;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private double doReadDouble(boolean raw) {
        in.position(raw ? rawOff : off);

        double val = in.readDouble();

        if (raw)
            rawOff += 8;
        else
            off += 8;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private char doReadChar(boolean raw) {
        in.position(raw ? rawOff : off);

        char val = in.readChar();

        if (raw)
            rawOff += 2;
        else
            off += 2;

        return val;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private boolean doReadBoolean(boolean raw) {
        in.position(raw ? rawOff++ : off++);

        return in.readBoolean();
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private BigDecimal doReadDecimal(boolean raw) {
        int scale = doReadInt(raw);
        byte[] mag = doReadByteArray(raw);

        BigInteger intVal = new BigInteger(mag);

        if (scale < 0) {
            scale &= 0x7FFFFFFF;

            intVal = intVal.negate();
        }

        return new BigDecimal(intVal, scale);
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private String doReadString(boolean raw) {
        if (doReadBoolean(raw)) {
            if (!in.hasArray())
                return new String(doReadByteArray(raw), UTF_8);

            int strLen = doReadInt(raw);
            int strOff = raw ? rawOff : off;

            String res = new String(in.array(), strOff, strLen, UTF_8);

            if (raw)
                rawOff += strLen;
            else
                off += strLen;

            return res;
        }
        else
            return String.valueOf(doReadCharArray(raw));
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private UUID doReadUuid(boolean raw) {
        return new UUID(doReadLong(raw), doReadLong(raw));
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Date doReadDate(boolean raw) {
        long time = doReadLong(raw);

        // Skip remainder.
        if (raw)
            rawOff += 4;
        else
            off += 4;

        return new Date(time);
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private Timestamp doReadTimestamp(boolean raw) {
        long time = doReadLong(raw);

        int nanos = doReadInt(raw);

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
        PortableReaderExImpl reader = new PortableReaderExImpl(ctx, in, raw ? rawOff : off, ldr, rCtx);

        Object obj = reader.deserialize();

        if (raw)
            rawOff += reader.len;
        else
            off += reader.len;

        return obj;
    }

    /**
     * @return Deserialized object.
     * @throws PortableException If failed.
     */
    @Nullable Object deserialize() throws PortableException {
        Object obj;

        byte flag = doReadByte(true);

        switch (flag) {
            case NULL:
                obj = null;

                break;

            case HANDLE:
                int handle = start - doReadInt(true);

                obj = rCtx.getObjectByHandle(handle);

                if (obj == null) {
                    off = handle;

                    obj = doReadObject(false);
                }

                break;

            case OBJ:
                if (typeId == null)
                    readObjectTypeId(true);

                assert typeId != UNREGISTERED_TYPE_ID;

                boolean userType = doReadBoolean(true);

                // Skip typeId and hash code.
                rawOff += 8;

                desc = ctx.descriptorForTypeId(userType, typeId, ldr);

                len = doReadInt(true);

                rawOff = start + doReadInt(true);

                if (desc == null)
                    throw new PortableInvalidClassException("Unknown type ID: " + typeId);

                obj = desc.read(this);

                break;

            case BYTE:
                obj = doReadByte(true);

                break;

            case SHORT:
                obj = doReadShort(true);

                break;

            case INT:
                obj = doReadInt(true);

                break;

            case LONG:
                obj = doReadLong(true);

                break;

            case FLOAT:
                obj = doReadFloat(true);

                break;

            case DOUBLE:
                obj = doReadDouble(true);

                break;

            case CHAR:
                obj = doReadChar(true);

                break;

            case BOOLEAN:
                obj = doReadBoolean(true);

                break;

            case DECIMAL:
                obj = doReadDecimal(true);

                break;

            case STRING:
                obj = doReadString(true);

                break;

            case UUID:
                obj = doReadUuid(true);

                break;

            case DATE:
                obj = isUseTimestamp() ? doReadTimestamp(true) : doReadDate(true);

                break;

            case BYTE_ARR:
                obj = doReadByteArray(true);

                break;

            case SHORT_ARR:
                obj = doReadShortArray(true);

                break;

            case INT_ARR:
                obj = doReadIntArray(true);

                break;

            case LONG_ARR:
                obj = doReadLongArray(true);

                break;

            case FLOAT_ARR:
                obj = doReadFloatArray(true);

                break;

            case DOUBLE_ARR:
                obj = doReadDoubleArray(true);

                break;

            case CHAR_ARR:
                obj = doReadCharArray(true);

                break;

            case BOOLEAN_ARR:
                obj = doReadBooleanArray(true);

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
                int len = doReadInt(true);

                ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

                try {
                    obj = ctx.optimizedMarsh().unmarshal(input, null);
                }
                catch (IgniteCheckedException e) {
                    throw new PortableException("Failed to unmarshal object with optimized marshaller", e);
                }

                rawOff += len;

                break;

            default:
                throw new PortableException("Invalid flag value: " + flag);
        }

        if (len == 0)
            len = rawOff - start;

        return obj;
    }

    /**
     * @return Use timestamp flag.
     * @throws PortableInvalidClassException If fails to find object type descriptor.
     */
    private boolean isUseTimestamp() throws PortableInvalidClassException {
        in.position(start);

        boolean dateObj = in.readByte() == DATE;

        if (!dateObj) {
            in.position(start + 2);

            int typeId = in.readInt(start + 2);

            return ctx.isUseTimestamp(typeId);
        }

        return ctx.isUseTimestamp();
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private byte[] doReadByteArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        byte[] arr = in.readByteArray(len);

        setHandler(arr, hPos);

        if (raw)
            rawOff += len;
        else
            off += len;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private short[] doReadShortArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        short[] arr = in.readShortArray(len);

        setHandler(arr, hPos);

        int bytes = len << 1;

        if (raw)
            rawOff += bytes;
        else
            off += bytes;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private int[] doReadIntArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        int[] arr = in.readIntArray(len);

        setHandler(arr, hPos);

        int bytes = len << 2;

        if (raw)
            rawOff += bytes;
        else
            off += bytes;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private long[] doReadLongArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        long[] arr = in.readLongArray(len);

        setHandler(arr, hPos);

        int bytes = len << 3;

        if (raw)
            rawOff += bytes;
        else
            off += bytes;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private float[] doReadFloatArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        float[] arr = in.readFloatArray(len);

        setHandler(arr, hPos);

        int bytes = len << 2;

        if (raw)
            rawOff += bytes;
        else
            off += bytes;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private double[] doReadDoubleArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        double[] arr = in.readDoubleArray(len);

        setHandler(arr, hPos);

        int bytes = len << 3;

        if (raw)
            rawOff += bytes;
        else
            off += bytes;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private char[] doReadCharArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        char[] arr = in.readCharArray(len);

        setHandler(arr, hPos);

        int bytes = len << 1;

        if (raw)
            rawOff += bytes;
        else
            off += bytes;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     */
    private boolean[] doReadBooleanArray(boolean raw) {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        in.position(raw ? rawOff : off);

        boolean[] arr = in.readBooleanArray(len);

        setHandler(arr, hPos);

        if (raw)
            rawOff += len;
        else
            off += len;

        return arr;
    }

    /**
     * @param raw Raw flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private BigDecimal[] doReadDecimalArray(boolean raw) throws PortableException {
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        BigDecimal[] arr = new BigDecimal[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = doReadByte(raw);

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != DECIMAL)
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadDecimal(raw);
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
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        String[] arr = new String[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = doReadByte(raw);

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != STRING)
                    throw new PortableException("Invalid flag value: " + flag);

                arr[i] = doReadString(raw);
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
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        UUID[] arr = new UUID[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = doReadByte(raw);

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
        int hPos = (raw ? rawOff : off) - 1;

        int len = doReadInt(raw);

        Date[] arr = new Date[len];

        setHandler(arr, hPos);

        for (int i = 0; i < len; i++) {
            byte flag = doReadByte(raw);

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
     * @param deep Deep flag.
     * @return Value.
     * @throws PortableException In case of error.
     */
    private Object[] doReadObjectArray(boolean raw, boolean deep) throws PortableException {
        int hPos = (raw ? rawOff : off) - 1;

        Class compType = doReadClass(raw);

        int len = doReadInt(raw);

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
        int hPos = (raw ? rawOff : off) - 1;

        int size = doReadInt(raw);

        assert size >= 0;

        byte colType = doReadByte(raw);

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
        int hPos = (raw ? rawOff : off) - 1;

        int size = doReadInt(raw);

        assert size >= 0;

        byte mapType = doReadByte(raw);

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
        int hPos = (raw ? rawOff : off) - 1;

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
            int len = doReadInt(raw);

            int pos = raw ? rawOff : off;

            if (raw)
                rawOff += len;
            else
                off += len;

            int start = doReadInt(raw);

            return new PortableObjectOffheapImpl(ctx, in.offheapPointer() + pos, start, len);
        }
        else {
            byte[] arr = doReadByteArray(raw);
            int start = doReadInt(raw);

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

        int ord = doReadInt(raw);

        return ord >= 0 ? (Enum<?>)GridEnumCache.get(cls)[ord] : null;
    }

    /**
     * @param raw Raw flag.
     * @param cls Enum class.
     * @return Value.
     */
    private Object[] doReadEnumArray(boolean raw, Class<?> cls) throws PortableException {
        int len = doReadInt(raw);

        Object[] arr = (Object[])Array.newInstance(cls, len);

        for (int i = 0; i < len; i++) {
            byte flag = doReadByte(raw);

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
        return doReadClass(raw, doReadInt(raw));
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
            byte flag = doReadByte(raw);

            if (flag != STRING)
                throw new PortableException("No class definition for typeId: " + typeId);

            String clsName = doReadString(raw);

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

        if (typeId == null)
            readObjectTypeId(false);

        assert typeId != UNREGISTERED_TYPE_ID;

        return ctx.fieldId(typeId, name);
    }

    /**
     * @param id Field ID.
     * @return Field offset.
     */
    private int fieldOffset(int id) {
        assert hdrLen != 0;

        int off = start + hdrLen;

        int end = start + in.readInt(start + RAW_DATA_OFF_POS);

        while (true) {
            if (off >= end)
                return -1;

            int id0 = in.readInt(off);

            if (id0 == id)
                return off + 8;

            int len = in.readInt(off + 4);

            off += (8 + len);
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
        in.position(rawOff);

        int cnt = in.read(b, off, len);

        if (cnt < len)
            throw new EOFException();

        rawOff += len;
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        int toSkip = Math.min(in.remaining(), n);

        in.position(in.position() + toSkip);

        rawOff += toSkip;

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
        in.position(rawOff);

        int cnt = in.read(b, off, len);

        rawOff += len;

        return cnt;
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
}
