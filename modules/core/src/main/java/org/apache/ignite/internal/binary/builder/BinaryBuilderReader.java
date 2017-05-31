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
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryPositionReadable;
import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.BinaryUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.charset.StandardCharsets.UTF_8;

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

    /**
     * Constructor.
     *
     * @param objImpl Binary object
     */
    BinaryBuilderReader(BinaryObjectImpl objImpl) {
        ctx = objImpl.context();
        arr = objImpl.array();
        pos = objImpl.start();

        reader = new BinaryReaderExImpl(ctx,
            BinaryHeapInputStream.create(arr, pos),
            ctx.configuration().getClassLoader(),
            false);

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

        reader = new BinaryReaderExImpl(ctx,
            BinaryHeapInputStream.create(arr, start),
            null,
            other.reader.handles(),
            false);

        this.objMap = other.objMap;
    }

    /**
     * @return Binary context.
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
     * @return Read length of array which was written in 'varint' encoding.
     */
    public int readLength() {
        return BinaryUtils.doReadUnsignedVarint(arr, pos);
    }

    /**
     * Read string length.
     *
     * @return String length.
     */
    public int readStringLength() {
        return BinaryUtils.doReadUnsignedVarint(arr, pos);
    }

    /**
     * Reads string.
     *
     * @return String.
     */
    public String readString() {
        byte flag = readByte();

        if (flag == GridBinaryMarshaller.NULL)
            return null;

        if (flag != GridBinaryMarshaller.STRING)
            throw new BinaryObjectException("Failed to deserialize String.");

        int len = readStringLength();

        pos += BinaryUtils.sizeInUnsignedVarint(len);

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
            case GridBinaryMarshaller.NULL:
                return;

            case GridBinaryMarshaller.OBJ:
                pos += readInt(GridBinaryMarshaller.TOTAL_LEN_POS - 1) - 1;

                return;

            case GridBinaryMarshaller.BOOLEAN:
            case GridBinaryMarshaller.BYTE:
                len = 1;
                break;

            case GridBinaryMarshaller.CHAR:
            case GridBinaryMarshaller.SHORT:
                len = 2;

                break;

            case GridBinaryMarshaller.HANDLE:
            case GridBinaryMarshaller.FLOAT:
            case GridBinaryMarshaller.INT:
                len = 4;

                break;

            case GridBinaryMarshaller.ENUM:
                //skipping type id and ordinal value
                len = 8;

                break;

            case GridBinaryMarshaller.LONG:
            case GridBinaryMarshaller.DOUBLE:
                len = 8;

                break;

            case GridBinaryMarshaller.BYTE_ARR:
            case GridBinaryMarshaller.BOOLEAN_ARR:
                len = readLength();
                len += BinaryUtils.sizeInUnsignedVarint(len);

                break;

            case GridBinaryMarshaller.STRING:
                len = readStringLength();
                len += BinaryUtils.sizeInUnsignedVarint(len);

                break;

            case GridBinaryMarshaller.DECIMAL:
                int magLen = BinaryUtils.doReadUnsignedVarint(arr,  pos);

                len = /** scale */ 4  + /** mag len */ BinaryUtils.sizeInUnsignedVarint(magLen)  + /** mag bytes count */ magLen;

                break;

            case GridBinaryMarshaller.UUID:
                len = 8 + 8;

                break;

            case GridBinaryMarshaller.DATE:
                len = 8;

                break;

            case GridBinaryMarshaller.TIMESTAMP:
                len = 8 + 4;

                break;

            case GridBinaryMarshaller.TIME:
                len = 8;

                break;

            case GridBinaryMarshaller.CHAR_ARR:
            case GridBinaryMarshaller.SHORT_ARR:
                len = readLength();
                len = len * 2 + BinaryUtils.sizeInUnsignedVarint(len);

                break;

            case GridBinaryMarshaller.INT_ARR:
            case GridBinaryMarshaller.FLOAT_ARR:
                len = readLength();
                len = len * 4 + BinaryUtils.sizeInUnsignedVarint(len);

                break;

            case GridBinaryMarshaller.LONG_ARR:
            case GridBinaryMarshaller.DOUBLE_ARR:
                len = readLength();
                len = len * 8 + BinaryUtils.sizeInUnsignedVarint(len);

                break;

            case GridBinaryMarshaller.DECIMAL_ARR:
            case GridBinaryMarshaller.DATE_ARR:
            case GridBinaryMarshaller.TIMESTAMP_ARR:
            case GridBinaryMarshaller.TIME_ARR:
            case GridBinaryMarshaller.OBJ_ARR:
            case GridBinaryMarshaller.ENUM_ARR:
            case GridBinaryMarshaller.UUID_ARR:
            case GridBinaryMarshaller.STRING_ARR: {
                int size = BinaryUtils.doReadUnsignedVarint(this);

                for (int i = 0; i < size; i++)
                    skipValue();

                return;
            }

            case GridBinaryMarshaller.COL: {
                int size = readInt();

                pos++; // skip collection type

                for (int i = 0; i < size; i++)
                    skipValue();

                return;
            }

            case GridBinaryMarshaller.MAP: {
                int size = readInt();

                pos++; // skip collection type

                for (int i = 0; i < size; i++) {
                    skipValue(); // skip key.
                    skipValue(); // skip value.
                }

                return;
            }

            case GridBinaryMarshaller.BINARY_OBJ:
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
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.HANDLE: {
                int objStart = pos - readIntPositioned(pos + 1);

                BinaryObjectBuilderImpl res = objMap.get(objStart);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, objStart), objStart);

                    objMap.put(objStart, res);
                }

                return res;
            }

            case GridBinaryMarshaller.OBJ: {
                BinaryObjectBuilderImpl res = objMap.get(pos);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, pos), pos);

                    objMap.put(pos, res);
                }

                return res;
            }

            case GridBinaryMarshaller.BYTE:
                return arr[pos + 1];

            case GridBinaryMarshaller.SHORT:
                return BinaryPrimitives.readShort(arr, pos + 1);

            case GridBinaryMarshaller.INT:
                return BinaryPrimitives.readInt(arr, pos + 1);

            case GridBinaryMarshaller.LONG:
                return BinaryPrimitives.readLong(arr, pos + 1);

            case GridBinaryMarshaller.FLOAT:
                return BinaryPrimitives.readFloat(arr, pos + 1);

            case GridBinaryMarshaller.DOUBLE:
                return BinaryPrimitives.readDouble(arr, pos + 1);

            case GridBinaryMarshaller.CHAR:
                return BinaryPrimitives.readChar(arr, pos + 1);

            case GridBinaryMarshaller.BOOLEAN:
                return arr[pos + 1] != 0;

            case GridBinaryMarshaller.DECIMAL:
            case GridBinaryMarshaller.STRING:
            case GridBinaryMarshaller.UUID:
            case GridBinaryMarshaller.DATE:
            case GridBinaryMarshaller.TIMESTAMP:
            case GridBinaryMarshaller.TIME:
                return new BinaryPlainLazyValue(this, pos, len);

            case GridBinaryMarshaller.BYTE_ARR:
            case GridBinaryMarshaller.SHORT_ARR:
            case GridBinaryMarshaller.INT_ARR:
            case GridBinaryMarshaller.LONG_ARR:
            case GridBinaryMarshaller.FLOAT_ARR:
            case GridBinaryMarshaller.DOUBLE_ARR:
            case GridBinaryMarshaller.CHAR_ARR:
            case GridBinaryMarshaller.BOOLEAN_ARR:
            case GridBinaryMarshaller.DECIMAL_ARR:
            case GridBinaryMarshaller.DATE_ARR:
            case GridBinaryMarshaller.TIMESTAMP_ARR:
            case GridBinaryMarshaller.TIME_ARR:
            case GridBinaryMarshaller.UUID_ARR:
            case GridBinaryMarshaller.STRING_ARR:
            case GridBinaryMarshaller.ENUM_ARR:
            case GridBinaryMarshaller.OBJ_ARR:
            case GridBinaryMarshaller.COL:
            case GridBinaryMarshaller.MAP:
                return new LazyCollection(pos);

            case GridBinaryMarshaller.ENUM: {
                if (len == 1) {
                    assert readByte(pos) == GridBinaryMarshaller.NULL;

                    return null;
                }

                int mark = position();
                position(pos + 1);

                BinaryBuilderEnum builderEnum = new BinaryBuilderEnum(this);

                position(mark);

                return builderEnum;
            }

            case GridBinaryMarshaller.BINARY_OBJ: {
                int size = readIntPositioned(pos + 1);

                int start = readIntPositioned(pos + 4 + size);

                BinaryObjectImpl binaryObj = new BinaryObjectImpl(ctx, arr, pos + 4 + start);

                return new BinaryPlainBinaryObject(binaryObj);
            }

            case GridBinaryMarshaller.OPTM_MARSH: {
                final BinaryHeapInputStream bin = BinaryHeapInputStream.create(arr, pos + 1);

                final Object obj = BinaryUtils.doReadOptimized(bin, ctx, U.resolveClassLoader(ctx.configuration()));

                return obj;
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
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.HANDLE: {
                int objStart = pos - 1 - readInt();

                BinaryObjectBuilderImpl res = objMap.get(objStart);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, objStart), objStart);

                    objMap.put(objStart, res);
                }

                return res;
            }

            case GridBinaryMarshaller.OBJ: {
                pos--;

                BinaryObjectBuilderImpl res = objMap.get(pos);

                if (res == null) {
                    res = new BinaryObjectBuilderImpl(new BinaryBuilderReader(this, pos), pos);

                    objMap.put(pos, res);
                }

                pos += readInt(GridBinaryMarshaller.TOTAL_LEN_POS);

                return res;
            }

            case GridBinaryMarshaller.BYTE:
                return arr[pos++];

            case GridBinaryMarshaller.SHORT: {
                Object res = BinaryPrimitives.readShort(arr, pos);
                pos += 2;
                return res;
            }

            case GridBinaryMarshaller.INT:
                return readInt();

            case GridBinaryMarshaller.LONG:
                plainLazyValLen = 8;

                break;

            case GridBinaryMarshaller.FLOAT:
                plainLazyValLen = 4;

                break;

            case GridBinaryMarshaller.DOUBLE:
                plainLazyValLen = 8;

                break;

            case GridBinaryMarshaller.CHAR:
                plainLazyValLen = 2;

                break;

            case GridBinaryMarshaller.BOOLEAN:
                return arr[pos++] != 0;

            case GridBinaryMarshaller.DECIMAL:
                int magLen = BinaryUtils.doReadUnsignedVarint(arr, pos + 4);

                plainLazyValLen = /** scale */ 4  + /** mag len */ BinaryUtils.sizeInUnsignedVarint(magLen)  + /** mag bytes count */ magLen;

                break;

            case GridBinaryMarshaller.STRING:
                plainLazyValLen = readStringLength();
                plainLazyValLen += BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);

                break;

            case GridBinaryMarshaller.UUID:
                plainLazyValLen = 8 + 8;

                break;

            case GridBinaryMarshaller.DATE:
                plainLazyValLen = 8;

                break;

            case GridBinaryMarshaller.TIMESTAMP:
                plainLazyValLen = 8 + 4;

                break;

            case GridBinaryMarshaller.TIME:
                plainLazyValLen = 8;

                break;

            case GridBinaryMarshaller.BYTE_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen += BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.SHORT_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen = plainLazyValLen * 2 + BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.INT_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen = plainLazyValLen * 4 + BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.LONG_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen = plainLazyValLen * 8 + BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.FLOAT_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen = plainLazyValLen * 4 + BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.DOUBLE_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen = plainLazyValLen * 8 + BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.CHAR_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen = plainLazyValLen * 2 + BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.BOOLEAN_ARR:
                plainLazyValLen = readLength();
                plainLazyValLen += BinaryUtils.sizeInUnsignedVarint(plainLazyValLen);
                modifiableLazyVal = true;

                break;

            case GridBinaryMarshaller.OBJ_ARR:
                return new BinaryObjectArrayLazyValue(this);

            case GridBinaryMarshaller.DATE_ARR: {
                int size = BinaryUtils.doReadUnsignedVarint(this);

                Date[] res = new Date[size];

                for (int i = 0; i < res.length; i++) {
                    byte flag = arr[pos++];

                    if (flag == GridBinaryMarshaller.NULL) continue;

                    if (flag != GridBinaryMarshaller.DATE)
                        throw new BinaryObjectException("Invalid flag value: " + flag);

                    long time = BinaryPrimitives.readLong(arr, pos);

                    pos += 8;

                    res[i] = new Date(time);
                }

                return res;
            }

            case GridBinaryMarshaller.TIMESTAMP_ARR: {
                int size = BinaryUtils.doReadUnsignedVarint(this);

                Timestamp[] res = new Timestamp[size];

                for (int i = 0; i < res.length; i++) {
                    byte flag = arr[pos++];

                    if (flag == GridBinaryMarshaller.NULL)
                        continue;

                    if (flag != GridBinaryMarshaller.TIMESTAMP)
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

            case GridBinaryMarshaller.TIME_ARR: {
                int size = BinaryUtils.doReadUnsignedVarint(this);

                Time[] res = new Time[size];

                for (int i = 0; i < res.length; i++) {
                    byte flag = arr[pos++];

                    if (flag == GridBinaryMarshaller.NULL) continue;

                    if (flag != GridBinaryMarshaller.TIME)
                        throw new BinaryObjectException("Invalid flag value: " + flag);

                    long time = BinaryPrimitives.readLong(arr, pos);

                    pos += 8;

                    res[i] = new Time(time);
                }

                return res;
            }

            case GridBinaryMarshaller.UUID_ARR:
            case GridBinaryMarshaller.STRING_ARR:
            case GridBinaryMarshaller.DECIMAL_ARR: {
                int size = BinaryUtils.doReadUnsignedVarint(this);

                for (int i = 0; i < size; i++) {
                    byte flag = arr[pos++];

                    if (flag == GridBinaryMarshaller.UUID)
                        pos += 8 + 8;
                    else if (flag == GridBinaryMarshaller.STRING) {
                        int strLen = readStringLength();
                        pos += BinaryUtils.sizeInUnsignedVarint(strLen);
                        pos += strLen;
                    }
                    else if (flag == GridBinaryMarshaller.DECIMAL) {
                        pos += 4; // scale value
                        int len = BinaryUtils.doReadUnsignedVarint(arr, pos);
                        pos += BinaryUtils.sizeInUnsignedVarint(len) + len;
                    }
                    else
                        assert flag == GridBinaryMarshaller.NULL;
                }

                return new BinaryModifiableLazyValue(this, valPos, pos - valPos);
            }

            case GridBinaryMarshaller.COL: {
                int size = readInt();
                byte colType = arr[pos++];

                switch (colType) {
                    case GridBinaryMarshaller.USER_COL:
                    case GridBinaryMarshaller.ARR_LIST:
                        return new BinaryLazyArrayList(this, size);

                    case GridBinaryMarshaller.LINKED_LIST:
                        return new BinaryLazyLinkedList(this, size);

                    case GridBinaryMarshaller.HASH_SET:
                    case GridBinaryMarshaller.LINKED_HASH_SET:
                        return new BinaryLazySet(this, size);
                }

                throw new BinaryObjectException("Unknown collection type: " + colType);
            }

            case GridBinaryMarshaller.MAP:
                return BinaryLazyMap.parseMap(this);

            case GridBinaryMarshaller.ENUM:
                return new BinaryBuilderEnum(this);

            case GridBinaryMarshaller.ENUM_ARR:
                return new BinaryEnumArrayLazyValue(this);

            case GridBinaryMarshaller.BINARY_OBJ: {
                int size = readInt();

                pos += size;

                int start = readInt();

                BinaryObjectImpl binaryObj = new BinaryObjectImpl(ctx, arr,
                    pos - 4 - size + start);

                return new BinaryPlainBinaryObject(binaryObj);
            }

            case GridBinaryMarshaller.OPTM_MARSH: {
                final BinaryHeapInputStream bin = BinaryHeapInputStream.create(arr, pos);

                final Object obj = BinaryUtils.doReadOptimized(bin, ctx, U.resolveClassLoader(ctx.configuration()));

                pos = bin.position();

                return obj;
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
