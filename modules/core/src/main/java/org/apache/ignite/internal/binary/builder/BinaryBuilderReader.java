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

package org.apache.ignite.internal.binary.builder;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.InternalBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryPositionReadable;
import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.binary.InternalBinaryMarshaller.NULL;
import static org.apache.ignite.internal.binary.InternalBinaryMarshaller.STRING;

/**
 *
 */
public class BinaryBuilderReader implements BinaryPositionReadable {
    /** */
    private final BinaryContext ctx;

    /** */
    private final byte[] arr;

    /** */
    private final BinaryReaderExImpl reader;

    /** */
    private final Map<Integer, BinaryObjectBuilderImpl> objMap;

    /** */
    private int pos;

    /*
     * Constructor.
     *
     * @param objImpl Portable object
     */
    BinaryBuilderReader(BinaryObjectImpl objImpl) {
        ctx = objImpl.context();
        arr = objImpl.array();
        pos = objImpl.start();

        // TODO: IGNITE-1272 - Is class loader needed here?
        reader = new BinaryReaderExImpl(ctx, BinaryHeapInputStream.create(arr, pos), null);

        objMap = new HashMap<>();
    }

    /**
     * Copying constructor.
     *
     * @param other Other reader.
     * @param start Start position.
     */
    BinaryBuilderReader(BinaryBuilderReader other, int start) {
        this.ctx = other.ctx;
        this.arr = other.arr;
        this.pos = start;

        reader = new BinaryReaderExImpl(ctx, BinaryHeapInputStream.create(arr, start), null, other.reader.handles());

        this.objMap = other.objMap;
    }

    /**
     * @return Portable context.
     */
    public BinaryContext binaryContext() {
        return ctx;
    }

    /**
     * @param obj Mutable binary object.
     */
    public void registerObject(BinaryObjectBuilderImpl obj) {
        objMap.put(obj.start(), obj);
    }

    /**
     * Get schema of the object, starting at the given position.
     *
     * @return Object's schema.
     */
    public BinarySchema schema() {
        return reader.getOrCreateSchema();
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
        return BinaryPrimitives.readInt(arr, pos + off);
    }

    /**
     * @param pos Position in the source array.
     * @return Read byte value.
     */
    public byte readBytePositioned(int pos) {
        return BinaryPrimitives.readByte(arr, pos);
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        return BinaryPrimitives.readShort(arr, pos);
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        return BinaryPrimitives.readInt(arr, pos);
    }

    /**
     * @return Read length of array.
     */
    public int readLength() {
        return BinaryPrimitives.readInt(arr, pos);
    }

    /**
     * Read string length.
     *
     * @return String length.
     */
    public int readStringLength() {
        return BinaryPrimitives.readInt(arr, pos);
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
            throw new BinaryObjectException("Failed to deserialize String.");

        int len = readInt();

        String str = new String(arr, pos, len, UTF_8);

        pos += len;

        return str;
    }

    /**
     *
     */
    public void skipValue() {
        byte type = arr[pos++];

        int len;

        switch (type) {
            case InternalBinaryMarshaller.NULL:
                return;

            case InternalBinaryMarshaller.OBJ:
                pos += readInt(InternalBinaryMarshaller.TOTAL_LEN_POS - 1) - 1;

                return;

            case InternalBinaryMarshaller.BOOLEAN:
            case InternalBinaryMarshaller.BYTE:
                len = 1;
                break;

            case InternalBinaryMarshaller.CHAR:
            case InternalBinaryMarshaller.SHORT:
                len = 2;

                break;

            case InternalBinaryMarshaller.HANDLE:
            case InternalBinaryMarshaller.FLOAT:
            case InternalBinaryMarshaller.INT:
                len = 4;

                break;

            case InternalBinaryMarshaller.ENUM:
                //skipping type id and ordinal value
                len = 8;

                break;

            case InternalBinaryMarshaller.LONG:
            case InternalBinaryMarshaller.DOUBLE:
                len = 8;

                break;

            case InternalBinaryMarshaller.BYTE_ARR:
            case InternalBinaryMarshaller.BOOLEAN_ARR:
                len = 4 + readLength();

                break;

            case InternalBinaryMarshaller.STRING:
                len = 4 + readStringLength();

                break;

            case InternalBinaryMarshaller.DECIMAL:
                len = /** scale */ 4  + /** mag len */ 4  + /** mag bytes count */ readInt(4);

                break;

            case InternalBinaryMarshaller.UUID:
                len = 8 + 8;

                break;

            case InternalBinaryMarshaller.DATE:
                len = 8;

                break;

            case InternalBinaryMarshaller.TIMESTAMP:
                len = 8 + 4;

                break;

            case InternalBinaryMarshaller.CHAR_ARR:
            case InternalBinaryMarshaller.SHORT_ARR:
                len = 4 + readLength() * 2;

                break;

            case InternalBinaryMarshaller.INT_ARR:
            case InternalBinaryMarshaller.FLOAT_ARR:
                len = 4 + readLength() * 4;

                break;

            case InternalBinaryMarshaller.LONG_ARR:
            case InternalBinaryMarshaller.DOUBLE_ARR:
                len = 4 + readLength() * 8;

                break;

            case InternalBinaryMarshaller.DECIMAL_ARR:
            case InternalBinaryMarshaller.DATE_ARR:
            case InternalBinaryMarshaller.TIMESTAMP_ARR:
            case InternalBinaryMarshaller.OBJ_ARR:
            case InternalBinaryMarshaller.ENUM_ARR:
            case InternalBinaryMarshaller.UUID_ARR:
            case InternalBinaryMarshaller.STRING_ARR: {
                int size = readInt();

                for (int i = 0; i < size; i++)
                    skipValue();

                return;
            }

            case InternalBinaryMarshaller.COL: {
                int size = readInt();

                pos++; // skip collection type

                for (int i = 0; i < size; i++)
                    skipValue();

                return;
            }

            case InternalBinaryMarshaller.MAP: {
                int size = readInt();

                pos++; // skip collection type

                for (int i = 0; i < size; i++) {
                    skipValue(); // skip key.
                    skipValue(); // skip value.
                }

                return;
            }

            case InternalBinaryMarshaller.MAP_ENTRY:
                skipValue();
                skipValue();

                return;

            case InternalBinaryMarshaller.BINARY_OBJ:
                len = readInt() + 4;

                break;

            default:
                throw new BinaryObjectException("Invalid flag value: " + type);
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
            case InternalBinaryMarshaller.NULL:
                return null;

            case InternalBinaryMarshaller.HANDLE: {
                int objStart = pos - readIntPositioned(pos + 1);

                BinaryObjectBuilderImpl res = objMap.get(objStart);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, objStart), objStart);

                    objMap.put(objStart, res);
                }

                return res;
            }

            case InternalBinaryMarshaller.OBJ: {
                BinaryObjectBuilderImpl res = objMap.get(pos);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, pos), pos);

                    objMap.put(pos, res);
                }

                return res;
            }

            case InternalBinaryMarshaller.BYTE:
                return arr[pos + 1];

            case InternalBinaryMarshaller.SHORT:
                return BinaryPrimitives.readShort(arr, pos + 1);

            case InternalBinaryMarshaller.INT:
                return BinaryPrimitives.readInt(arr, pos + 1);

            case InternalBinaryMarshaller.LONG:
                return BinaryPrimitives.readLong(arr, pos + 1);

            case InternalBinaryMarshaller.FLOAT:
                return BinaryPrimitives.readFloat(arr, pos + 1);

            case InternalBinaryMarshaller.DOUBLE:
                return BinaryPrimitives.readDouble(arr, pos + 1);

            case InternalBinaryMarshaller.CHAR:
                return BinaryPrimitives.readChar(arr, pos + 1);

            case InternalBinaryMarshaller.BOOLEAN:
                return arr[pos + 1] != 0;

            case InternalBinaryMarshaller.DECIMAL:
            case InternalBinaryMarshaller.STRING:
            case InternalBinaryMarshaller.UUID:
            case InternalBinaryMarshaller.DATE:
            case InternalBinaryMarshaller.TIMESTAMP:
                return new BinaryPlainLazyValue(this, pos, len);

            case InternalBinaryMarshaller.BYTE_ARR:
            case InternalBinaryMarshaller.SHORT_ARR:
            case InternalBinaryMarshaller.INT_ARR:
            case InternalBinaryMarshaller.LONG_ARR:
            case InternalBinaryMarshaller.FLOAT_ARR:
            case InternalBinaryMarshaller.DOUBLE_ARR:
            case InternalBinaryMarshaller.CHAR_ARR:
            case InternalBinaryMarshaller.BOOLEAN_ARR:
            case InternalBinaryMarshaller.DECIMAL_ARR:
            case InternalBinaryMarshaller.DATE_ARR:
            case InternalBinaryMarshaller.TIMESTAMP_ARR:
            case InternalBinaryMarshaller.UUID_ARR:
            case InternalBinaryMarshaller.STRING_ARR:
            case InternalBinaryMarshaller.ENUM_ARR:
            case InternalBinaryMarshaller.OBJ_ARR:
            case InternalBinaryMarshaller.COL:
            case InternalBinaryMarshaller.MAP:
            case InternalBinaryMarshaller.MAP_ENTRY:
                return new LazyCollection(pos);

            case InternalBinaryMarshaller.ENUM: {
                if (len == 1) {
                    assert readByte(pos) == InternalBinaryMarshaller.NULL;

                    return null;
                }

                int mark = position();
                position(pos + 1);

                BinaryBuilderEnum builderEnum = new BinaryBuilderEnum(this);

                position(mark);

                return builderEnum;
            }

            case InternalBinaryMarshaller.BINARY_OBJ: {
                int size = readIntPositioned(pos + 1);

                int start = readIntPositioned(pos + 4 + size);

                BinaryObjectImpl binaryObj = new BinaryObjectImpl(ctx, arr, pos + 4 + start);

                return new PlainBinaryObject(binaryObj);
            }

            default:
                throw new BinaryObjectException("Invalid flag value: " + type);
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
            case InternalBinaryMarshaller.NULL:
                return null;

            case InternalBinaryMarshaller.HANDLE: {
                int objStart = pos - 1 - readInt();

                BinaryObjectBuilderImpl res = objMap.get(objStart);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, objStart), objStart);

                    objMap.put(objStart, res);
                }

                return res;
            }

            case InternalBinaryMarshaller.OBJ: {
                pos--;

                BinaryObjectBuilderImpl res = objMap.get(pos);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, pos), pos);

                    objMap.put(pos, res);
                }

                pos += readInt(InternalBinaryMarshaller.TOTAL_LEN_POS);

                return res;
            }

            case InternalBinaryMarshaller.BYTE:
                return arr[pos++];

            case InternalBinaryMarshaller.SHORT: {
                Object res = BinaryPrimitives.readShort(arr, pos);
                pos += 2;
                return res;
            }

            case InternalBinaryMarshaller.INT:
                return readInt();

            case InternalBinaryMarshaller.LONG:
                plainLazyValLen = 8;

                break;

            case InternalBinaryMarshaller.FLOAT:
                plainLazyValLen = 4;

                break;

            case InternalBinaryMarshaller.DOUBLE:
                plainLazyValLen = 8;

                break;

            case InternalBinaryMarshaller.CHAR:
                plainLazyValLen = 2;

                break;

            case InternalBinaryMarshaller.BOOLEAN:
                return arr[pos++] != 0;

            case InternalBinaryMarshaller.DECIMAL:
                plainLazyValLen = /** scale */ 4  + /** mag len */ 4  + /** mag bytes count */ readInt(4);

                break;

            case InternalBinaryMarshaller.STRING:
                plainLazyValLen = 4 + readStringLength();

                break;

            case InternalBinaryMarshaller.UUID:
                plainLazyValLen = 8 + 8;

                break;

            case InternalBinaryMarshaller.DATE:
                plainLazyValLen = 8;

                break;

            case InternalBinaryMarshaller.TIMESTAMP:
                plainLazyValLen = 8 + 4;

                break;

            case InternalBinaryMarshaller.BYTE_ARR:
                plainLazyValLen = 4 + readLength();
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.SHORT_ARR:
                plainLazyValLen = 4 + readLength() * 2;
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.INT_ARR:
                plainLazyValLen = 4 + readLength() * 4;
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.LONG_ARR:
                plainLazyValLen = 4 + readLength() * 8;
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.FLOAT_ARR:
                plainLazyValLen = 4 + readLength() * 4;
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.DOUBLE_ARR:
                plainLazyValLen = 4 + readLength() * 8;
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.CHAR_ARR:
                plainLazyValLen = 4 + readLength() * 2;
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.BOOLEAN_ARR:
                plainLazyValLen = 4 + readLength();
                modifiableLazyVal = true;

                break;

            case InternalBinaryMarshaller.OBJ_ARR:
                return new BinaryObjectArrayLazyValue(this);

            case InternalBinaryMarshaller.DATE_ARR: {
                int size = readInt();

                Date[] res = new Date[size];

                for (int i = 0; i < res.length; i++) {
                    byte flag = arr[pos++];

                    if (flag == InternalBinaryMarshaller.NULL) continue;

                    if (flag != InternalBinaryMarshaller.DATE)
                        throw new BinaryObjectException("Invalid flag value: " + flag);

                    long time = BinaryPrimitives.readLong(arr, pos);

                    pos += 8;

                    res[i] = new Date(time);
                }

                return res;
            }

            case InternalBinaryMarshaller.TIMESTAMP_ARR: {
                int size = readInt();

                Timestamp[] res = new Timestamp[size];

                for (int i = 0; i < res.length; i++) {
                    byte flag = arr[pos++];

                    if (flag == InternalBinaryMarshaller.NULL)
                        continue;

                    if (flag != InternalBinaryMarshaller.TIMESTAMP)
                        throw new BinaryObjectException("Invalid flag value: " + flag);

                    long time = BinaryPrimitives.readLong(arr, pos);

                    pos += 8;

                    int nano = BinaryPrimitives.readInt(arr, pos);

                    pos += 4;

                    Timestamp ts = new Timestamp(time);

                    ts.setNanos(ts.getNanos() + nano);

                    res[i] = ts;
                }

                return res;
            }

            case InternalBinaryMarshaller.UUID_ARR:
            case InternalBinaryMarshaller.STRING_ARR:
            case InternalBinaryMarshaller.DECIMAL_ARR: {
                int size = readInt();

                for (int i = 0; i < size; i++) {
                    byte flag = arr[pos++];

                    if (flag == InternalBinaryMarshaller.UUID)
                        pos += 8 + 8;
                    else if (flag == InternalBinaryMarshaller.STRING)
                        pos += 4 + readStringLength();
                    else if (flag == InternalBinaryMarshaller.DECIMAL) {
                        pos += 4; // scale value
                        pos += 4 + readLength();
                    }
                    else
                        assert flag == InternalBinaryMarshaller.NULL;
                }

                return new BinaryModifiableLazyValue(this, valPos, pos - valPos);
            }

            case InternalBinaryMarshaller.COL: {
                int size = readInt();
                byte colType = arr[pos++];

                switch (colType) {
                    case InternalBinaryMarshaller.USER_COL:
                    case InternalBinaryMarshaller.ARR_LIST:
                        return new BinaryLazyArrayList(this, size);

                    case InternalBinaryMarshaller.LINKED_LIST:
                        return new BinaryLazyLinkedList(this, size);

                    case InternalBinaryMarshaller.HASH_SET:
                    case InternalBinaryMarshaller.LINKED_HASH_SET:
                    case InternalBinaryMarshaller.TREE_SET:
                    case InternalBinaryMarshaller.CONC_SKIP_LIST_SET:
                        return new BinaryLazySet(this, size);
                }

                throw new BinaryObjectException("Unknown collection type: " + colType);
            }

            case InternalBinaryMarshaller.MAP:
                return BinaryLazyMap.parseMap(this);

            case InternalBinaryMarshaller.ENUM:
                return new BinaryBuilderEnum(this);

            case InternalBinaryMarshaller.ENUM_ARR:
                return new BinaryEnumArrayLazyValue(this);

            case InternalBinaryMarshaller.MAP_ENTRY:
                return new BinaryLazyMapEntry(this);

            case InternalBinaryMarshaller.BINARY_OBJ: {
                int size = readInt();

                pos += size;

                int start = readInt();

                BinaryObjectImpl binaryObj = new BinaryObjectImpl(ctx, arr,
                    pos - 4 - size + start);

                return new PlainBinaryObject(binaryObj);
            }

            default:
                throw new BinaryObjectException("Invalid flag value: " + type);
        }

        BinaryAbstractLazyValue res;

        if (modifiableLazyVal)
            res = new BinaryModifiableLazyValue(this, valPos, 1 + plainLazyValLen);
        else
            res = new BinaryPlainLazyValue(this, valPos, 1 + plainLazyValLen);

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
    BinaryReaderExImpl reader() {
        return reader;
    }

    /**
     *
     */
    private class LazyCollection implements BinaryLazyValue {
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
        @Override public void writeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer ctx) {
            ctx.writeValue(writer, wrappedCollection());
        }

        /** {@inheritDoc} */
        @Override public Object value() {
            return BinaryUtils.unwrapLazy(wrappedCollection());
        }
    }
}
