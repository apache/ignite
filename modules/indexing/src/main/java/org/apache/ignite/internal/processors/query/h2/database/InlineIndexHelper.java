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

package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampUtc;
import org.h2.value.ValueUuid;

/**
 * Helper class for in-page indexes.
 */
public class InlineIndexHelper {
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    /** PageContext for use in IO's */
    private static final ThreadLocal<List<InlineIndexHelper>> currentIndex = new ThreadLocal<>();

    /** */
    public static final List<Integer> AVAILABLE_TYPES = Arrays.asList(
        Value.BOOLEAN,
        Value.BYTE,
        Value.SHORT,
        Value.INT,
        Value.LONG,
        Value.LONG,
        Value.FLOAT,
        Value.DOUBLE,
        Value.DATE,
        Value.TIME,
        Value.TIMESTAMP,
        Value.TIMESTAMP_UTC,
        Value.UUID,
        Value.STRING,
        Value.STRING_FIXED,
        Value.STRING_IGNORECASE,
        Value.BYTES
    );

    /** */
    private final int type;

    /** */
    private final int colIdx;

    /** */
    private final int sortType;

    /**
     * @param type Index type (see {@link Value}).
     * @param colIdx Index column index.
     * @param sortType Column sort type (see {@link IndexColumn#sortType}).
     */
    public InlineIndexHelper(int type, int colIdx, int sortType) {
        this.type = type;
        this.colIdx = colIdx;
        this.sortType = sortType;
    }

    /**
     * @return Index type.
     */
    public int type() {
        return type;
    }

    /**
     * @return Column index.
     */
    public int columnIndex() {
        return colIdx;
    }

    /**
     * @return Sort type.
     */
    public int sortType() {
        return sortType;
    }

    /**
     * @return Page context for current thread.
     */
    public static List<InlineIndexHelper> getCurrentInlineIndexes() {
        return currentIndex.get();
    }

    /**
     * Sets page context for current thread.
     */
    public static void setCurrentInlineIndexes(List<InlineIndexHelper> inlineIdxs) {
        currentIndex.set(inlineIdxs);
    }

    /**
     * Clears current context.
     */
    public static void clearCurrentInlineIndexes() {
        currentIndex.remove();
    }

    /**
     * @return Value size.
     */
    public short size() {
        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
                return 1;

            case Value.SHORT:
                return 2;

            case Value.INT:
                return 4;

            case Value.LONG:
                return 8;

            case Value.FLOAT:
                return Float.SIZE;

            case Value.DOUBLE:
                return Float.SIZE;

            case Value.DATE:
                return 8;

            case Value.TIME:
                return 8;

            case Value.TIMESTAMP:
                return 16;

            case Value.TIMESTAMP_UTC:
                return 8;

            case Value.UUID:
                return 16;

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
            case Value.BYTES:
                return -1;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Full size in page.
     */
    public int fullSize(long pageAddr, int off) {
        int type = PageUtils.getByte(pageAddr, off);

        if (type == Value.NULL)
            return 1;

        if (size() > 0)
            return size() + 1;
        else
            return PageUtils.getShort(pageAddr, off + 1) + 3;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Value.
     */
    public Value get(long pageAddr, int off, int maxSize) {
        if (size() > 0 && size() + 1 > maxSize)
            return null;

        int type = PageUtils.getByte(pageAddr, off);

        if (type == Value.UNKNOWN)
            return null;

        if (type == Value.NULL)
            return ValueNull.INSTANCE;

        if (this.type != type)
            throw new UnsupportedOperationException("invalid fast index type " + type);

        switch (this.type) {
            case Value.BOOLEAN:
                return ValueBoolean.get(PageUtils.getByte(pageAddr, off + 1) != 0);

            case Value.BYTE:
                return ValueByte.get(PageUtils.getByte(pageAddr, off + 1));

            case Value.SHORT:
                return ValueShort.get(PageUtils.getShort(pageAddr, off + 1));

            case Value.INT:
                return ValueInt.get(PageUtils.getInt(pageAddr, off + 1));

            case Value.LONG:
                return ValueLong.get(PageUtils.getLong(pageAddr, off + 1));

            case Value.FLOAT: {
                byte[] bytes = PageUtils.getBytes(pageAddr, off + 1, size());
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                return ValueFloat.get(buf.getFloat());
            }

            case Value.DOUBLE: {
                byte[] bytes = PageUtils.getBytes(pageAddr, off + 1, size());
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                return ValueDouble.get(buf.getDouble());
            }

            case Value.TIME:
                return ValueTime.fromNanos(PageUtils.getLong(pageAddr, off + 1));

            case Value.DATE:
                return ValueDate.fromDateValue(PageUtils.getLong(pageAddr, off + 1));

            case Value.TIMESTAMP:
                return ValueTimestamp.fromDateValueAndNanos(PageUtils.getLong(pageAddr, off + 1), PageUtils.getLong(pageAddr, off + 9));

            case Value.TIMESTAMP_UTC:
                return ValueTimestampUtc.fromNanos(PageUtils.getLong(pageAddr, off + 1));

            case Value.UUID:
                return ValueUuid.get(PageUtils.getLong(pageAddr, off + 1), PageUtils.getLong(pageAddr, off + 9));

            case Value.STRING: {
                int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
                return ValueString.get(new String(PageUtils.getBytes(pageAddr, off + 3, size), CHARSET));
            }

            case Value.STRING_FIXED: {
                int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
                return ValueStringFixed.get(new String(PageUtils.getBytes(pageAddr, off + 3, size), CHARSET));
            }

            case Value.STRING_IGNORECASE: {
                int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
                return ValueStringIgnoreCase.get(new String(PageUtils.getBytes(pageAddr, off + 3, size), CHARSET));
            }

            case Value.BYTES: {
                int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
                return ValueBytes.get(PageUtils.getBytes(pageAddr, off + 3, size));
            }

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return {@code True} if string is not truncated on save.
     */
    protected boolean isValueFull(long pageAddr, int off) {
        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
            case Value.INT:
            case Value.SHORT:
            case Value.LONG:
                return true;

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
            case Value.BYTES:
                return (PageUtils.getShort(pageAddr, off + 1) & 0x8000) == 0;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param maxSize Maximum size to read.
     * @param v Value to compare.
     * @param comp Comparator.
     * @return Compare result
     */
    public int compare(long pageAddr, int off, int maxSize, Value v, Comparator<Value> comp) {
        Value v1 = get(pageAddr, off, maxSize);

        if (v1 == null)
            return -2;

        int c = comp.compare(v1, v);
        assert c > -2;

        if (size() < 0)
            return sortType() == SortOrder.DESCENDING ? -c : c;
        else {
            if (isValueFull(pageAddr, off) || canRelyOnCompare(c, v1, v))
                return sortType() == SortOrder.DESCENDING ? -c : c;
            else
                return -2;
        }
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param val Value.
     * @return NUmber of bytes saved.
     */
    public int put(long pageAddr, int off, Value val, int maxSize) {
        if (size() > 0 && size() + 1 > maxSize)
            return 0;

        if (val.getType() == Value.NULL) {
            PageUtils.putByte(pageAddr, off, (byte)Value.NULL);
            return 1;
        }

        if (val.getType() != type)
            throw new UnsupportedOperationException("value type doesn't match");

        switch (type) {
            case Value.BOOLEAN:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putByte(pageAddr, off + 1, (byte)(val.getBoolean() ? 1 : 0));
                return size() + 1;

            case Value.BYTE:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putByte(pageAddr, off + 1, val.getByte());
                return size() + 1;

            case Value.SHORT:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putShort(pageAddr, off + 1, val.getShort());
                return size() + 1;

            case Value.INT:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putInt(pageAddr, off + 1, val.getInt());
                return size() + 1;

            case Value.LONG:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, val.getLong());
                return size() + 1;

            case Value.FLOAT: {
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                byte[] bytes = new byte[size()];
                ByteBuffer.wrap(bytes).putFloat(val.getFloat());
                PageUtils.putBytes(pageAddr, off + 1, bytes);
                return size() + 1;
            }

            case Value.DOUBLE: {
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                byte[] bytes = new byte[size()];
                ByteBuffer.wrap(bytes).putDouble(val.getDouble());
                PageUtils.putBytes(pageAddr, off + 1, bytes);
                return size() + 1;
            }

            case Value.TIME:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTime)val).getNanos());
                return size() + 1;

            case Value.DATE:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueDate)val).getDateValue());
                return size() + 1;

            case Value.TIMESTAMP:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTimestamp)val).getDateValue());
                PageUtils.putLong(pageAddr, off + 9, ((ValueTimestamp)val).getTimeNanos());
                return size() + 1;

            case Value.TIMESTAMP_UTC:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTimestampUtc)val).getUtcDateTimeNanos());
                return size() + 1;

            case Value.UUID:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueUuid)val).getHigh());
                PageUtils.putLong(pageAddr, off + 9, ((ValueUuid)val).getLow());
                return size() + 1;

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE: {
                byte[] s;
                short size;

                if (val.getString().getBytes(CHARSET).length + 3 <= maxSize) {
                    s = val.getString().getBytes(CHARSET);
                    size = (short)s.length;
                }
                else {
                    s = toBytes(val.getString(), maxSize - 3);
                    size = (short)(s.length | 0x8000);
                }

                if (s == null) {
                    // Can't fit anything to
                    PageUtils.putByte(pageAddr, off, (byte)Value.UNKNOWN);
                    return 0;
                }
                else {
                    PageUtils.putByte(pageAddr, off, (byte)val.getType());
                    PageUtils.putShort(pageAddr, off + 1, size);
                    PageUtils.putBytes(pageAddr, off + 3, s);
                    return s.length + 3;
                }
            }

            case Value.BYTES: {
                byte[] s;
                short size;

                PageUtils.putByte(pageAddr, off, (byte)val.getType());

                if (val.getBytes().length + 3 <= maxSize) {
                    size = (short)val.getBytes().length;
                    PageUtils.putShort(pageAddr, off + 1, size);
                    PageUtils.putBytes(pageAddr, off + 3, val.getBytes());
                }
                else {
                    size = (short)((maxSize - 3) | 0x8000);
                    PageUtils.putShort(pageAddr, off + 1, size);
                    PageUtils.putBytes(pageAddr, off + 3, Arrays.copyOfRange(val.getBytes(), 0, maxSize - 3));
                }
                return size + 3;
            }

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * Convert String to byte[] with size limit, according to UTF-8 encoding.
     *
     * @param s String.
     * @param limit Size limit.
     * @return byte[].
     */
    public static byte[] toBytes(String s, int limit) {
        byte[] bytes = s.getBytes(CHARSET);
        if (bytes.length <= limit)
            return bytes;

        for (int i = bytes.length - 1; i > 0; i--) {
            if ((bytes[i] & 0xc0) != 0x80 && i <= limit) {
                byte[] res = new byte[i];
                System.arraycopy(bytes, 0, res, 0, i);
                return res;
            }
        }

        return null;
    }

    /**
     * @param c Compare result.
     * @param shortVal Short value.
     * @param v2 Second value;
     * @return {@code true} if we can rely on compare result.
     */
    protected boolean canRelyOnCompare(int c, Value shortVal, Value v2) {
        switch (type) {
            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                if (c == 0 && shortVal.getType() != Value.NULL && v2.getType() != Value.NULL)
                    return false;

                if (shortVal.getType() != Value.NULL
                    && v2.getType() != Value.NULL
                    && c < 0
                    && shortVal.getString().length() <= v2.getString().length()) {
                    // Can't rely on compare, should use full string.
                    return false;
                }
                return true;

            case Value.BYTES:
                if (c == 0 && shortVal.getType() != Value.NULL && v2.getType() != Value.NULL)
                    return false;

                if (shortVal.getType() != Value.NULL
                    && v2.getType() != Value.NULL
                    && c < 0
                    && shortVal.getBytes().length <= v2.getBytes().length) {
                    // Can't rely on compare, should use full array.
                    return false;
                }
                return true;

            default:
                return true;
        }
    }
}
