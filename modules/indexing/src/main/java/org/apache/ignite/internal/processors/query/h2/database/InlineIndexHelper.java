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

    /** */
    private final short size;

    /**
     * @param type Index type (see {@link Value}).
     * @param colIdx Index column index.
     * @param sortType Column sort type (see {@link IndexColumn#sortType}).
     */
    public InlineIndexHelper(int type, int colIdx, int sortType) {
        this.type = type;
        this.colIdx = colIdx;
        this.sortType = sortType;

        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
                this.size = 1;
                break;

            case Value.SHORT:
                this.size = 2;
                break;

            case Value.INT:
                this.size = 4;
                break;

            case Value.LONG:
                this.size = 8;
                break;

            case Value.FLOAT:
                this.size = 4;
                break;

            case Value.DOUBLE:
                this.size = 8;
                break;

            case Value.DATE:
                this.size = 8;
                break;

            case Value.TIME:
                this.size = 8;
                break;

            case Value.TIMESTAMP:
                this.size = 16;
                break;

            case Value.UUID:
                this.size = 16;
                break;

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
            case Value.BYTES:
                this.size = -1;
                break;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
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
        return size;
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

        if (size > 0)
            return size + 1;
        else
            return PageUtils.getShort(pageAddr, off + 1) + 3;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Value.
     */
    public Value get(long pageAddr, int off, int maxSize) {
        if (size > 0 && size + 1 > maxSize)
            return null;

        if (maxSize < 1)
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
                return ValueFloat.get(Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1)));
            }

            case Value.DOUBLE: {
                return ValueDouble.get(Double.longBitsToDouble(PageUtils.getLong(pageAddr, off + 1)));
            }

            case Value.TIME:
                return ValueTime.fromNanos(PageUtils.getLong(pageAddr, off + 1));

            case Value.DATE:
                return ValueDate.fromDateValue(PageUtils.getLong(pageAddr, off + 1));

            case Value.TIMESTAMP:
                return ValueTimestamp.fromDateValueAndNanos(PageUtils.getLong(pageAddr, off + 1), PageUtils.getLong(pageAddr, off + 9));

            case Value.UUID:
                return ValueUuid.get(PageUtils.getLong(pageAddr, off + 1), PageUtils.getLong(pageAddr, off + 9));

            case Value.STRING:
                return ValueString.get(new String(readBytes(pageAddr, off), CHARSET));

            case Value.STRING_FIXED:
                return ValueStringFixed.get(new String(readBytes(pageAddr, off), CHARSET));

            case Value.STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(new String(readBytes(pageAddr, off), CHARSET));

            case Value.BYTES:
                return ValueBytes.get(readBytes(pageAddr, off));

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /** Read variable length bytearray */
    private static byte[] readBytes(long pageAddr, int off) {
        int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
        return PageUtils.getBytes(pageAddr, off + 3, size);
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
     * @return Compare result (-2 means we can't compare).
     */
    public int compare(long pageAddr, int off, int maxSize, Value v, Comparator<Value> comp) {
        Value v1 = get(pageAddr, off, maxSize);

        if (v1 == null)
            return -2;

        int c = comp.compare(v1, v);
        c = c != 0 ? c > 0 ? 1 : -1 : 0;

        if (size > 0)
            return fixSort(c, sortType());

        if (isValueFull(pageAddr, off) || canRelyOnCompare(c, v1, v))
            return fixSort(c, sortType());

        return -2;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param val Value.
     * @return NUmber of bytes saved.
     */
    public int put(long pageAddr, int off, Value val, int maxSize) {
        if (size > 0 && size + 1 > maxSize)
            return 0;

        if (size < 0 && maxSize < 4) {
            // can't fit vartype field
            PageUtils.putByte(pageAddr, off, (byte)Value.UNKNOWN);
            return 0;
        }

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
                return size + 1;

            case Value.BYTE:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putByte(pageAddr, off + 1, val.getByte());
                return size + 1;

            case Value.SHORT:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putShort(pageAddr, off + 1, val.getShort());
                return size + 1;

            case Value.INT:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putInt(pageAddr, off + 1, val.getInt());
                return size + 1;

            case Value.LONG:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, val.getLong());
                return size + 1;

            case Value.FLOAT: {
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putInt(pageAddr, off + 1, Float.floatToIntBits(val.getFloat()));
                return size + 1;
            }

            case Value.DOUBLE: {
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, Double.doubleToLongBits(val.getDouble()));
                return size + 1;
            }

            case Value.TIME:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTime)val).getNanos());
                return size + 1;

            case Value.DATE:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueDate)val).getDateValue());
                return size + 1;

            case Value.TIMESTAMP:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTimestamp)val).getDateValue());
                PageUtils.putLong(pageAddr, off + 9, ((ValueTimestamp)val).getTimeNanos());
                return size + 1;

            case Value.UUID:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueUuid)val).getHigh());
                PageUtils.putLong(pageAddr, off + 9, ((ValueUuid)val).getLow());
                return size + 1;

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE: {
                short size;

                byte[] s = val.getString().getBytes(CHARSET);
                if (s.length + 3 <= maxSize)
                    size = (short)s.length;
                else {
                    s = trimUTF8(s, maxSize - 3);
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
     * @param bytes byte[].
     * @param limit Size limit.
     * @return byte[].
     */
    public static byte[] trimUTF8(byte[] bytes, int limit) {
        if (bytes.length <= limit)
            return bytes;

        for (int i = limit; i > 0; i--) {
            if ((bytes[i] & 0xc0) != 0x80) {
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
            case Value.BYTES:
                if (shortVal.getType() == Value.NULL || v2.getType() == Value.NULL)
                    return true;

                if (c == 0 && shortVal.getType() != Value.NULL && v2.getType() != Value.NULL)
                    return false;

                int l1;
                int l2;

                if (type == Value.BYTES) {
                    l1 = shortVal.getBytes().length;
                    l2 = v2.getBytes().length;
                }
                else {
                    l1 = shortVal.getString().length();
                    l2 = v2.getString().length();
                }

                if (c < 0 && l1 <= l2) {
                    // Can't rely on compare, should use full value.
                    return false;
                }

                return true;

            default:
                return true;
        }
    }

    /**
     * Perform sort order correction.
     *
     * @param c Compare result.
     * @param sortType Sort type.
     * @return Fixed compare result.
     */
    public static int fixSort(int c, int sortType) {
        return sortType == SortOrder.ASCENDING ? c : -c;
    }
}
