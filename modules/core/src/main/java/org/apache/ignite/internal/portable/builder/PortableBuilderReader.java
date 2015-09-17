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

package org.apache.ignite.internal.portable.builder;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.PortableObjectImpl;
import org.apache.ignite.internal.portable.PortablePrimitives;
import org.apache.ignite.internal.portable.PortableReaderExImpl;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.portable.PortableWriterExImpl;
import org.apache.ignite.internal.portable.api.PortableException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;

/**
 *
 */
class PortableBuilderReader {
    /** */
    private static final PortablePrimitives PRIM = PortablePrimitives.get();

    /** */
    private final Map<Integer, PortableBuilderImpl> objMap = new HashMap<>();

    /** */
    private final PortableContext ctx;

    /** */
    private final PortableReaderExImpl reader;

    /** */
    private byte[] arr;

    /** */
    private int pos;

    /**
     * @param objImpl Portable object
     */
    PortableBuilderReader(PortableObjectImpl objImpl) {
        ctx = objImpl.context();
        arr = objImpl.array();
        pos = objImpl.start();

        // TODO: IGNITE-1272 - Is class loader needed here?
        reader = new PortableReaderExImpl(portableContext(), arr, pos, null);
    }

    /**
     * @return Portable context.
     */
    public PortableContext portableContext() {
        return ctx;
    }

    /**
     * @param obj Mutable portable object.
     */
    public void registerObject(PortableBuilderImpl obj) {
        objMap.put(obj.start(), obj);
    }

    /**
     * @return Read int value.
     */
    public int readInt() {
        int res = readInt(0);

        pos += 4;

        return res;
    }

    /**
     * @return Read int value.
     */
    public byte readByte() {
        return arr[pos++];
    }

    /**
     * @return Read boolean value.
     */
    public boolean readBoolean() {
        return readByte() == 1;
    }

    /**
     * @return Read int value.
     */
    public byte readByte(int off) {
        return arr[pos + off];
    }

    /**
     * @param off Offset related to {@link #pos}
     * @return Read int value.
     */
    public int readInt(int off) {
        return PRIM.readInt(arr, pos + off);
    }

    /**
     * @param pos Position in the source array.
     * @return Read int value.
     */
    public int readIntAbsolute(int pos) {
        return PRIM.readInt(arr, pos);
    }

    /**
     * @return Read length of array.
     */
    public int readLength() {
        return PRIM.readInt(arr, pos);
    }

    /**
     * Read string length.
     *
     * @return String length.
     */
    public int readStringLength() {
        boolean utf = PRIM.readBoolean(arr, pos);

        int arrLen = PRIM.readInt(arr, pos + 1);

        return 1 + (utf ? arrLen : arrLen << 1);
    }

    /**
     * Reads string.
     *
     * @return String.
     */
    public String readString() {
        byte flag = readByte();

        if (flag == NULL)
            return null;

        if (flag != STRING)
            throw new PortableException("Failed to deserialize String.");

        boolean convert = readBoolean();
        int len = readInt();

        String str;

        if (convert) {
            str = new String(arr, pos, len, UTF_8);

            pos += len;
        }
        else {
            str = String.valueOf(PRIM.readCharArray(arr, pos, len));

            pos += len << 1;
        }

        return str;
    }

    /**
     *
     */
    public void skipValue() {
        byte type = arr[pos++];

        int len;

        switch (type) {
            case GridPortableMarshaller.NULL:
                return;

            case GridPortableMarshaller.OBJ:
                pos += readInt(GridPortableMarshaller.TOTAL_LEN_POS - 1) - 1;

                return;

            case GridPortableMarshaller.BOOLEAN:
            case GridPortableMarshaller.BYTE:
                len = 1;
                break;

            case GridPortableMarshaller.CHAR:
            case GridPortableMarshaller.SHORT:
                len = 2;

                break;

            case GridPortableMarshaller.HANDLE:
            case GridPortableMarshaller.FLOAT:
            case GridPortableMarshaller.INT:
                len = 4;

                break;

            case GridPortableMarshaller.ENUM:
                //skipping type id and ordinal value
                len = 8;

                break;

            case GridPortableMarshaller.LONG:
            case GridPortableMarshaller.DOUBLE:
                len = 8;

                break;

            case GridPortableMarshaller.BYTE_ARR:
            case GridPortableMarshaller.BOOLEAN_ARR:
                len = 4 + readLength();

                break;

            case GridPortableMarshaller.STRING:
                len = 4 + readStringLength();

                break;

            case GridPortableMarshaller.DECIMAL:
                len = /** scale */ 4  + /** mag len */ 4  + /** mag bytes count */ readInt(4);

                break;

            case GridPortableMarshaller.UUID:
                len = 8 + 8;

                break;

            case GridPortableMarshaller.DATE:
                len = 8 + 4;

                break;

            case GridPortableMarshaller.CHAR_ARR:
            case GridPortableMarshaller.SHORT_ARR:
                len = 4 + readLength() * 2;

                break;

            case GridPortableMarshaller.INT_ARR:
            case GridPortableMarshaller.FLOAT_ARR:
                len = 4 + readLength() * 4;

                break;

            case GridPortableMarshaller.LONG_ARR:
            case GridPortableMarshaller.DOUBLE_ARR:
                len = 4 + readLength() * 8;

                break;

            case GridPortableMarshaller.DECIMAL_ARR:
            case GridPortableMarshaller.DATE_ARR:
            case GridPortableMarshaller.OBJ_ARR:
            case GridPortableMarshaller.ENUM_ARR:
            case GridPortableMarshaller.UUID_ARR:
            case GridPortableMarshaller.STRING_ARR: {
                int size = readInt();

                for (int i = 0; i < size; i++)
                    skipValue();

                return;
            }

            case GridPortableMarshaller.COL: {
                int size = readInt();

                pos++; // skip collection type

                for (int i = 0; i < size; i++)
                    skipValue();

                return;
            }

            case GridPortableMarshaller.MAP: {
                int size = readInt();

                pos++; // skip collection type

                for (int i = 0; i < size; i++) {
                    skipValue(); // skip key.
                    skipValue(); // skip value.
                }

                return;
            }

            case GridPortableMarshaller.MAP_ENTRY:
                skipValue();
                skipValue();

                return;

            case GridPortableMarshaller.PORTABLE_OBJ:
                len = readInt() + 4;

                break;

            default:
                throw new PortableException("Invalid flag value: " + type);
        }

        pos += len;
    }

    /**
     * @param pos Position.
     * @param len Length.
     * @return Object.
     */
    public Object getValueQuickly(int pos, int len) {
        byte type = arr[pos];

        switch (type) {
            case GridPortableMarshaller.NULL:
                return null;

            case GridPortableMarshaller.HANDLE: {
                int objStart = pos - readIntAbsolute(pos + 1);

                PortableBuilderImpl res = objMap.get(objStart);

                if (res == null) {
                    res = new PortableBuilderImpl(this, objStart);

                    objMap.put(objStart, res);
                }

                return res;
            }

            case GridPortableMarshaller.OBJ: {
                PortableBuilderImpl res = objMap.get(pos);

                if (res == null) {
                    res = new PortableBuilderImpl(this, pos);

                    objMap.put(pos, res);
                }

                return res;
            }

            case GridPortableMarshaller.BYTE:
                return arr[pos + 1];

            case GridPortableMarshaller.SHORT:
                return PRIM.readShort(arr, pos + 1);

            case GridPortableMarshaller.INT:
                return PRIM.readInt(arr, pos + 1);

            case GridPortableMarshaller.LONG:
                return PRIM.readLong(arr, pos + 1);

            case GridPortableMarshaller.FLOAT:
                return PRIM.readFloat(arr, pos + 1);

            case GridPortableMarshaller.DOUBLE:
                return PRIM.readDouble(arr, pos + 1);

            case GridPortableMarshaller.CHAR:
                return PRIM.readChar(arr, pos + 1);

            case GridPortableMarshaller.BOOLEAN:
                return arr[pos + 1] != 0;

            case GridPortableMarshaller.DECIMAL:
            case GridPortableMarshaller.STRING:
            case GridPortableMarshaller.UUID:
            case GridPortableMarshaller.DATE:
                return new PortablePlainLazyValue(this, pos, len);

            case GridPortableMarshaller.BYTE_ARR:
            case GridPortableMarshaller.SHORT_ARR:
            case GridPortableMarshaller.INT_ARR:
            case GridPortableMarshaller.LONG_ARR:
            case GridPortableMarshaller.FLOAT_ARR:
            case GridPortableMarshaller.DOUBLE_ARR:
            case GridPortableMarshaller.CHAR_ARR:
            case GridPortableMarshaller.BOOLEAN_ARR:
            case GridPortableMarshaller.DECIMAL_ARR:
            case GridPortableMarshaller.DATE_ARR:
            case GridPortableMarshaller.UUID_ARR:
            case GridPortableMarshaller.STRING_ARR:
            case GridPortableMarshaller.ENUM_ARR:
            case GridPortableMarshaller.OBJ_ARR:
            case GridPortableMarshaller.COL:
            case GridPortableMarshaller.MAP:
            case GridPortableMarshaller.MAP_ENTRY:
                return new LazyCollection(pos);

            case GridPortableMarshaller.ENUM: {
                if (len == 1) {
                    assert readByte(pos) == GridPortableMarshaller.NULL;

                    return null;
                }

                int mark = position();
                position(pos + 1);

                PortableBuilderEnum builderEnum = new PortableBuilderEnum(this);

                position(mark);

                return builderEnum;
            }

            case GridPortableMarshaller.PORTABLE_OBJ: {
                int size = readIntAbsolute(pos + 1);

                int start = readIntAbsolute(pos + 4 + size);

                PortableObjectImpl portableObj = new PortableObjectImpl(ctx, arr, pos + 4 + start);

                return new PortablePlainPortableObject(portableObj);
            }

            default:
                throw new PortableException("Invalid flag value: " + type);
        }
    }

    /**
     * @return Parsed value.
     */
    public Object parseValue() {
        int valPos = pos;

        byte type = arr[pos++];

        int plainLazyValLen;

        boolean modifiableLazyVal = false;

        switch (type) {
            case GridPortableMarshaller.NULL:
                return null;

            case GridPortableMarshaller.HANDLE: {
                int objStart = pos - 1 - readInt();

                PortableBuilderImpl res = objMap.get(objStart);

                if (res == null) {
                    res = new PortableBuilderImpl(this, objStart);

                    objMap.put(objStart, res);
                }

                return res;
            }

            case GridPortableMarshaller.OBJ: {
                pos--;

                PortableBuilderImpl res = objMap.get(pos);

                if (res == null) {
                    res = new PortableBuilderImpl(this, pos);

                    objMap.put(pos, res);
                }

                pos += readInt(GridPortableMarshaller.TOTAL_LEN_POS);

                return res;
            }

            case GridPortableMarshaller.BYTE:
                return arr[pos++];

            case GridPortableMarshaller.SHORT: {
                Object res = PRIM.readShort(arr, pos);
                pos += 2;
                return res;
            }

            case GridPortableMarshaller.INT:
                return readInt();

            case GridPortableMarshaller.LONG:
                plainLazyValLen = 8;

                break;

            case GridPortableMarshaller.FLOAT:
                plainLazyValLen = 4;

                break;

            case GridPortableMarshaller.DOUBLE:
                plainLazyValLen = 8;

                break;

            case GridPortableMarshaller.CHAR:
                plainLazyValLen = 2;

                break;

            case GridPortableMarshaller.BOOLEAN:
                return arr[pos++] != 0;

            case GridPortableMarshaller.DECIMAL:
                plainLazyValLen = /** scale */ 4  + /** mag len */ 4  + /** mag bytes count */ readInt(4);

                break;

            case GridPortableMarshaller.STRING:
                plainLazyValLen = 4 + readStringLength();

                break;

            case GridPortableMarshaller.UUID:
                plainLazyValLen = 8 + 8;

                break;

            case GridPortableMarshaller.DATE:
                plainLazyValLen = 8 + 4;

                break;

            case GridPortableMarshaller.BYTE_ARR:
                plainLazyValLen = 4 + readLength();
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.SHORT_ARR:
                plainLazyValLen = 4 + readLength() * 2;
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.INT_ARR:
                plainLazyValLen = 4 + readLength() * 4;
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.LONG_ARR:
                plainLazyValLen = 4 + readLength() * 8;
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.FLOAT_ARR:
                plainLazyValLen = 4 + readLength() * 4;
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.DOUBLE_ARR:
                plainLazyValLen = 4 + readLength() * 8;
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.CHAR_ARR:
                plainLazyValLen = 4 + readLength() * 2;
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.BOOLEAN_ARR:
                plainLazyValLen = 4 + readLength();
                modifiableLazyVal = true;

                break;

            case GridPortableMarshaller.OBJ_ARR:
                return new PortableObjectArrayLazyValue(this);

            case GridPortableMarshaller.DATE_ARR: {
                int size = readInt();

                Date[] res = new Date[size];

                for (int i = 0; i < res.length; i++) {
                    byte flag = arr[pos++];

                    if (flag == GridPortableMarshaller.NULL) continue;

                    if (flag != GridPortableMarshaller.DATE)
                        throw new PortableException("Invalid flag value: " + flag);

                    long time = PRIM.readLong(arr, pos);

                    pos += 8;

                    if (ctx.isUseTimestamp()) {
                        Timestamp ts = new Timestamp(time);

                        ts.setNanos(ts.getNanos() + readInt());

                        res[i] = ts;
                    }
                    else {
                        res[i] = new Date(time);

                        pos += 4;
                    }
                }

                return res;
            }

            case GridPortableMarshaller.UUID_ARR:
            case GridPortableMarshaller.STRING_ARR:
            case GridPortableMarshaller.DECIMAL_ARR: {
                int size = readInt();

                for (int i = 0; i < size; i++) {
                    byte flag = arr[pos++];

                    if (flag == GridPortableMarshaller.UUID)
                        pos += 8 + 8;
                    else if (flag == GridPortableMarshaller.STRING)
                        pos += 4 + readStringLength();
                    else if (flag == GridPortableMarshaller.DECIMAL) {
                        pos += 4; // scale value
                        pos += 4 + readLength();
                    }
                    else
                        assert flag == GridPortableMarshaller.NULL;
                }

                return new PortableModifiableLazyValue(this, valPos, pos - valPos);
            }

            case GridPortableMarshaller.COL: {
                int size = readInt();
                byte colType = arr[pos++];

                switch (colType) {
                    case GridPortableMarshaller.USER_COL:
                    case GridPortableMarshaller.ARR_LIST:
                        return new PortableLazyArrayList(this, size);

                    case GridPortableMarshaller.LINKED_LIST:
                        return new PortableLazyLinkedList(this, size);

                    case GridPortableMarshaller.HASH_SET:
                    case GridPortableMarshaller.LINKED_HASH_SET:
                    case GridPortableMarshaller.TREE_SET:
                    case GridPortableMarshaller.CONC_SKIP_LIST_SET:
                        return new PortableLazySet(this, size);
                }

                throw new PortableException("Unknown collection type: " + colType);
            }

            case GridPortableMarshaller.MAP:
                return PortableLazyMap.parseMap(this);

            case GridPortableMarshaller.ENUM:
                return new PortableBuilderEnum(this);

            case GridPortableMarshaller.ENUM_ARR:
                return new PortableEnumArrayLazyValue(this);

            case GridPortableMarshaller.MAP_ENTRY:
                return new PortableLazyMapEntry(this);

            case GridPortableMarshaller.PORTABLE_OBJ: {
                int size = readInt();

                pos += size;

                int start = readInt();

                PortableObjectImpl portableObj = new PortableObjectImpl(ctx, arr,
                    pos - 4 - size + start);

                return new PortablePlainPortableObject(portableObj);
            }


            default:
                throw new PortableException("Invalid flag value: " + type);
        }

        PortableAbstractLazyValue res;

        if (modifiableLazyVal)
            res = new PortableModifiableLazyValue(this, valPos, 1 + plainLazyValLen);
        else
            res = new PortablePlainLazyValue(this, valPos, 1 + plainLazyValLen);

        pos += plainLazyValLen;

        return res;
    }

    /**
     * @return Array.
     */
    public byte[] array() {
        return arr;
    }

    /**
     * @return Position of reader.
     */
    public int position() {
        return pos;
    }

    /**
     * @param pos New pos.
     */
    public void position(int pos) {
        this.pos = pos;
    }

    /**
     * @param n Number of bytes to skip.
     */
    public void skip(int n) {
        pos += n;
    }

    /**
     * @return Reader.
     */
    PortableReaderExImpl reader() {
        return reader;
    }

    /**
     *
     */
    private class LazyCollection implements PortableLazyValue {
        /** */
        private final int valOff;

        /** */
        private Object col;

        /**
         * @param valOff Value.
         */
        protected LazyCollection(int valOff) {
            this.valOff = valOff;
        }

        /**
         * @return Object.
         */
        private Object wrappedCollection() {
            if (col == null) {
                position(valOff);

                col = parseValue();
            }

            return col;
        }

        /** {@inheritDoc} */
        @Override public void writeTo(PortableWriterExImpl writer, PortableBuilderSerializer ctx) {
            ctx.writeValue(writer, wrappedCollection());
        }

        /** {@inheritDoc} */
        @Override public Object value() {
            return PortableUtils.unwrapLazy(wrappedCollection());
        }
    }
}