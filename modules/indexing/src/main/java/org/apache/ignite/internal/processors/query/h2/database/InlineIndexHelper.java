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
import org.apache.ignite.internal.util.GridUnsafe;
import org.h2.value.CompareMode;
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
        IndexValueType.BOOLEAN,
        IndexValueType.BYTE,
        IndexValueType.SHORT,
        IndexValueType.INT,
        IndexValueType.LONG,
        IndexValueType.FLOAT,
        IndexValueType.DOUBLE,
        IndexValueType.DATE,
        IndexValueType.TIME,
        IndexValueType.TIMESTAMP,
        IndexValueType.UUID,
        IndexValueType.STRING,
        IndexValueType.STRING_FIXED,
        IndexValueType.STRING_IGNORECASE,
        IndexValueType.BYTES
    );

    /** */
    private final int type;

    /** */
    private final int colIdx;

    /** */
    private final boolean descending;

    /** */
    private final short size;

    /** */
    private final boolean compareBinaryUnsigned;

    /** */
    private final boolean compareStringsOptimized;

    /**
     * @param type Index type.
     * @param colIdx Index column index.
     * @param descending Whether column is sorted in descending order.
     */
    public InlineIndexHelper(int type, int colIdx, boolean descending, CompareMode compareMode) {
        this.type = type;
        this.colIdx = colIdx;
        this.descending = descending;

        this.compareBinaryUnsigned = compareMode.isBinaryUnsigned();

        // Optimized strings comparison can be used only if there are no custom collators.
        // H2 internal comparison will be used otherwise (may be slower).
        this.compareStringsOptimized = CompareMode.OFF.equals(compareMode.getName());

        switch (type) {
            case IndexValueType.BOOLEAN:
            case IndexValueType.BYTE:
                this.size = 1;
                break;

            case IndexValueType.SHORT:
                this.size = 2;
                break;

            case IndexValueType.INT:
                this.size = 4;
                break;

            case IndexValueType.LONG:
                this.size = 8;
                break;

            case IndexValueType.FLOAT:
                this.size = 4;
                break;

            case IndexValueType.DOUBLE:
                this.size = 8;
                break;

            case IndexValueType.DATE:
                this.size = 8;
                break;

            case IndexValueType.TIME:
                this.size = 8;
                break;

            case IndexValueType.TIMESTAMP:
                this.size = 16;
                break;

            case IndexValueType.UUID:
                this.size = 16;
                break;

            case IndexValueType.STRING:
            case IndexValueType.STRING_FIXED:
            case IndexValueType.STRING_IGNORECASE:
            case IndexValueType.BYTES:
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

        if (type == IndexValueType.NULL)
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

        if (type == IndexValueType.UNKNOWN)
            return null;

        if (type == IndexValueType.NULL)
            return ValueNull.INSTANCE;

        if (this.type != type)
            throw new UnsupportedOperationException("invalid fast index type " + type);

        switch (this.type) {
            case IndexValueType.BOOLEAN:
                return ValueBoolean.get(PageUtils.getByte(pageAddr, off + 1) != 0);

            case IndexValueType.BYTE:
                return ValueByte.get(PageUtils.getByte(pageAddr, off + 1));

            case IndexValueType.SHORT:
                return ValueShort.get(PageUtils.getShort(pageAddr, off + 1));

            case IndexValueType.INT:
                return ValueInt.get(PageUtils.getInt(pageAddr, off + 1));

            case IndexValueType.LONG:
                return ValueLong.get(PageUtils.getLong(pageAddr, off + 1));

            case IndexValueType.FLOAT: {
                return ValueFloat.get(Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1)));
            }

            case IndexValueType.DOUBLE: {
                return ValueDouble.get(Double.longBitsToDouble(PageUtils.getLong(pageAddr, off + 1)));
            }

            case IndexValueType.TIME:
                return ValueTime.fromNanos(PageUtils.getLong(pageAddr, off + 1));

            case IndexValueType.DATE:
                return ValueDate.fromDateValue(PageUtils.getLong(pageAddr, off + 1));

            case IndexValueType.TIMESTAMP:
                return ValueTimestamp.fromDateValueAndNanos(PageUtils.getLong(pageAddr, off + 1), PageUtils.getLong(pageAddr, off + 9));

            case IndexValueType.UUID:
                return ValueUuid.get(PageUtils.getLong(pageAddr, off + 1), PageUtils.getLong(pageAddr, off + 9));

            case IndexValueType.STRING:
                return ValueString.get(new String(readBytes(pageAddr, off), CHARSET));

            case IndexValueType.STRING_FIXED:
                return ValueStringFixed.get(new String(readBytes(pageAddr, off), CHARSET));

            case IndexValueType.STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(new String(readBytes(pageAddr, off), CHARSET));

            case IndexValueType.BYTES:
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
            case IndexValueType.BOOLEAN:
            case IndexValueType.BYTE:
            case IndexValueType.INT:
            case IndexValueType.SHORT:
            case IndexValueType.LONG:
                return true;

            case IndexValueType.STRING:
            case IndexValueType.STRING_FIXED:
            case IndexValueType.STRING_IGNORECASE:
            case IndexValueType.BYTES:
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
        int c = tryCompareOptimized(pageAddr, off, maxSize, v);

        if (c != Integer.MIN_VALUE)
            return c;

        Value v1 = get(pageAddr, off, maxSize);

        if (v1 == null)
            return -2;

        c = Integer.signum(comp.compare(v1, v));

        if (size > 0)
            return fixSort(c);

        if (isValueFull(pageAddr, off) || canRelyOnCompare(c, v1, v))
            return fixSort(c);

        return -2;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param maxSize Maximum size to read.
     * @param v Value to compare.
     * @return Compare result ({@code Integer.MIN_VALUE} means unsupported operation; {@code -2} - can't compare).
     */
    private int tryCompareOptimized(long pageAddr, int off, int maxSize, Value v) {
        int type;

        if ((size > 0 && size + 1 > maxSize)
                || maxSize < 1
                || (type = PageUtils.getByte(pageAddr, off)) == IndexValueType.UNKNOWN)
            return -2;

        if (type == IndexValueType.NULL)
            return Integer.MIN_VALUE;

        if (v == ValueNull.INSTANCE)
            return fixSort(1);

        if (this.type != type)
            throw new UnsupportedOperationException("Invalid fast index type: " + type);

        type = Value.getHigherOrder(type, v.getType());

        switch (type) {
            case IndexValueType.BOOLEAN:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                boolean bool1 = PageUtils.getByte(pageAddr, off + 1) != 0;
                boolean bool2 = v.getBoolean();

                return fixSort(Boolean.compare(bool1, bool2));

            case IndexValueType.BYTE:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                byte byte1 = PageUtils.getByte(pageAddr, off + 1);
                byte byte2 = v.getByte();

                return fixSort(Byte.compare(byte1, byte2));

            case IndexValueType.SHORT:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                short short1 = PageUtils.getShort(pageAddr, off + 1);
                short short2 = v.getShort();

                return fixSort(Short.compare(short1, short2));

            case IndexValueType.INT:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                int int1 = PageUtils.getInt(pageAddr, off + 1);
                int int2 = v.getInt();

                return fixSort(Integer.compare(int1, int2));

            case IndexValueType.LONG:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                long long1 = PageUtils.getLong(pageAddr, off + 1);
                long long2 = v.getLong();

                return fixSort(Long.compare(long1, long2));

            case IndexValueType.FLOAT:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                float float1 = Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1));
                float float2 = v.getFloat();

                return fixSort(Float.compare(float1, float2));

            case IndexValueType.DOUBLE:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                double double1 = Double.longBitsToDouble(PageUtils.getLong(pageAddr, off + 1));
                double double2 = v.getDouble();

                return fixSort(Double.compare(double1, double2));

            case IndexValueType.TIME:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                long nanos1 = PageUtils.getLong(pageAddr, off + 1);
                long nanos2 = ((ValueTime)v.convertTo(Value.TIME)).getNanos();

                return fixSort(Long.compare(nanos1, nanos2));

            case IndexValueType.DATE:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                long date1 = PageUtils.getLong(pageAddr, off + 1);
                long date2 = ((ValueDate)v.convertTo(Value.DATE)).getDateValue();

                return fixSort(Long.compare(date1, date2));

            case IndexValueType.TIMESTAMP:
                if (PageUtils.getByte(pageAddr, off) != type)
                    return Integer.MIN_VALUE;

                ValueTimestamp v0 = (ValueTimestamp)v.convertTo(Value.TIMESTAMP);

                date1 = PageUtils.getLong(pageAddr, off + 1);
                date2 = v0.getDateValue();

                int c = Long.compare(date1, date2);

                if (c == 0) {
                    nanos1 = PageUtils.getLong(pageAddr, off + 9);
                    nanos2 = v0.getTimeNanos();

                    c = Long.compare(nanos1, nanos2);
                }

                return fixSort(c);

            case IndexValueType.STRING:
            case IndexValueType.STRING_FIXED:
            case IndexValueType.STRING_IGNORECASE:
                if (compareStringsOptimized)
                    return compareAsString(pageAddr, off, v.getString(), type == IndexValueType.STRING_IGNORECASE);

                break;

            case IndexValueType.BYTES:
                return compareAsBytes(pageAddr, off, v.getBytesNoCopy());
        }

        return Integer.MIN_VALUE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param bytes Bytes to compare to.
     * @return Compare result ({@code -2} means we can't compare).
     */
    private int compareAsBytes(long pageAddr, int off, byte[] bytes) {
        int len1;

        long addr = pageAddr + off + 1; // Skip type.

        if(size > 0)
            // Fixed size value.
            len1 = size;
        else {
            len1 = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;

            addr += 2; // Skip size.
        }

        int len2 = bytes.length;

        int len = Math.min(len1, len2);

        if (compareBinaryUnsigned) {
            for (int i = 0; i < len; i++) {
                int b1 = GridUnsafe.getByte(addr + i) & 0xff;
                int b2 = bytes[i] & 0xff;

                if (b1 != b2)
                    return fixSort(Integer.signum(b1 - b2));
            }
        }
        else {
            for (int i = 0; i < len; i++) {
                byte b1 = GridUnsafe.getByte(addr + i);
                byte b2 = bytes[i];

                if (b1 != b2)
                    return fixSort(Integer.signum(b1 - b2));
            }
        }

        int res = Integer.signum(len1 - len2);

        if(isValueFull(pageAddr, off))
            return fixSort(res);

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return fixSort(1);

        return -2;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param s String to compare.
     * @param ignoreCase {@code True} if a case-insensitive comparison should be used.
     * @return Compare result ({@code -2} means we can't compare).
     */
    private int compareAsString(long pageAddr, int off, String s, boolean ignoreCase) {
        int len1 = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
        int len2 = s.length();

        int c, c2, c3, c4, cntr1 = 0, cntr2 = 0;
        char v1, v2;

        long addr = pageAddr + off + 3; // Skip length and type byte.

        // Try reading ASCII.
        while (cntr1 < len1 && cntr2 < len2) {
            c = (int) GridUnsafe.getByte(addr) & 0xFF;

            if (c > 127)
                break;

            cntr1++; addr++;

            v1 = (char)c;
            v2 = s.charAt(cntr2++);

            if (ignoreCase) {
                v1 = Character.toUpperCase(v1);
                v2 = Character.toUpperCase(v2);
            }

            if (v1 != v2)
                return fixSort(Integer.signum(v1 - v2));
        }

        // read other
        while (cntr1 < len1 && cntr2 < len2) {
            c = (int) GridUnsafe.getByte(addr++) & 0xFF;

            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                /* 0xxxxxxx*/
                    cntr1++;

                    v1 = (char)c;

                    break;

                case 12:
                case 13:
                /* 110x xxxx   10xx xxxx*/
                    cntr1 += 2;

                    if (cntr1 > len1)
                        throw new IllegalStateException("Malformed input (partial character at the end).");

                    c2 = (int) GridUnsafe.getByte(addr++) & 0xFF;

                    if ((c2 & 0xC0) != 0x80)
                        throw new IllegalStateException("Malformed input around byte: " + (cntr1 - 2));

                    c = c & 0x1F;
                    c = (c << 6) | (c2 & 0x3F);

                    v1 = (char)c;

                    break;

                case 14:
                /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    cntr1 += 3;

                    if (cntr1 > len1)
                        throw new IllegalStateException("Malformed input (partial character at the end).");

                    c2 = (int) GridUnsafe.getByte(addr++) & 0xFF;

                    c3 = (int) GridUnsafe.getByte(addr++) & 0xFF;

                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80))
                        throw new IllegalStateException("Malformed input around byte: " + (cntr1 - 3));

                    c = c & 0x0F;
                    c = (c << 6) | (c2 & 0x3F);
                    c = (c << 6) | (c3 & 0x3F);

                    v1 = (char)c;

                    break;

                case 15:
                /* 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx */
                    cntr1 += 4;

                    if (cntr1 > len1)
                        throw new IllegalStateException("Malformed input (partial character at the end).");

                    c2 = (int) GridUnsafe.getByte(addr++) & 0xFF;

                    c3 = (int) GridUnsafe.getByte(addr++) & 0xFF;

                    c4 = (int) GridUnsafe.getByte(addr++) & 0xFF;

                    if (((c & 0xF8) != 0xf0) || ((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80) || ((c4 & 0xC0) != 0x80))
                    throw new IllegalStateException("Malformed input around byte: " + (cntr1 - 4));

                    c = c & 0x07;
                    c = (c << 6) | (c2 & 0x3F);
                    c = (c << 6) | (c3 & 0x3F);
                    c = (c << 6) | (c4 & 0x3F);

                    c = c - 0x010000; // Subtract 0x010000, c is now 0..fffff (20 bits)

                    // height surrogate
                    v1 = (char)(0xD800 + ((c >> 10) & 0x7FF));
                    v2 = s.charAt(cntr2++);

                    if (v1 != v2)
                        return fixSort(Integer.signum(v1 - v2));

                    if (cntr2 == len2)
                        // The string is malformed (partial partial character at the end).
                        // Finish comparison here.
                        return fixSort(1);

                    // Low surrogate.
                    v1 = (char)(0xDC00 + (c & 0x3FF));
                    v2 = s.charAt(cntr2++);

                    if (v1 != v2)
                        return fixSort(Integer.signum(v1 - v2));

                    continue;

                default:
                /* 10xx xxxx */
                    throw new IllegalStateException("Malformed input around byte: " + cntr1);
            }

            v2 = s.charAt(cntr2++);

            if (ignoreCase) {
                v1 = Character.toUpperCase(v1);
                v2 = Character.toUpperCase(v2);
            }

            if (v1 != v2)
                return fixSort(Integer.signum(v1 - v2));
        }

        int res = cntr1 == len1 && cntr2 == len2 ? 0 : cntr1 == len1 ? -1 : 1;

        if (isValueFull(pageAddr, off))
            return fixSort(res);

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return fixSort(1);

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
            PageUtils.putByte(pageAddr, off, (byte)IndexValueType.UNKNOWN);
            return 0;
        }

        if (val.getType() == IndexValueType.NULL) {
            PageUtils.putByte(pageAddr, off, (byte)IndexValueType.NULL);
            return 1;
        }

        if (val.getType() != type)
            throw new UnsupportedOperationException("value type doesn't match");

        switch (type) {
            case IndexValueType.BOOLEAN:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putByte(pageAddr, off + 1, (byte)(val.getBoolean() ? 1 : 0));
                return size + 1;

            case IndexValueType.BYTE:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putByte(pageAddr, off + 1, val.getByte());
                return size + 1;

            case IndexValueType.SHORT:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putShort(pageAddr, off + 1, val.getShort());
                return size + 1;

            case IndexValueType.INT:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putInt(pageAddr, off + 1, val.getInt());
                return size + 1;

            case IndexValueType.LONG:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, val.getLong());
                return size + 1;

            case IndexValueType.FLOAT: {
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putInt(pageAddr, off + 1, Float.floatToIntBits(val.getFloat()));
                return size + 1;
            }

            case IndexValueType.DOUBLE: {
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, Double.doubleToLongBits(val.getDouble()));
                return size + 1;
            }

            case IndexValueType.TIME:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTime)val).getNanos());
                return size + 1;

            case IndexValueType.DATE:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueDate)val).getDateValue());
                return size + 1;

            case IndexValueType.TIMESTAMP:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueTimestamp)val).getDateValue());
                PageUtils.putLong(pageAddr, off + 9, ((ValueTimestamp)val).getTimeNanos());
                return size + 1;

            case IndexValueType.UUID:
                PageUtils.putByte(pageAddr, off, (byte)val.getType());
                PageUtils.putLong(pageAddr, off + 1, ((ValueUuid)val).getHigh());
                PageUtils.putLong(pageAddr, off + 9, ((ValueUuid)val).getLow());
                return size + 1;

            case IndexValueType.STRING:
            case IndexValueType.STRING_FIXED:
            case IndexValueType.STRING_IGNORECASE: {
                short size;

                byte[] s = val.getString().getBytes(CHARSET);
                if (s.length + 3 <= maxSize)
                    size = (short)s.length;
                else {
                    s = trimUTF8(s, maxSize - 3);
                    size = (short)(s == null ? 0 : s.length | 0x8000);
                }

                if (s == null) {
                    // Can't fit anything to
                    PageUtils.putByte(pageAddr, off, (byte)IndexValueType.UNKNOWN);
                    return 0;
                }
                else {
                    PageUtils.putByte(pageAddr, off, (byte)val.getType());
                    PageUtils.putShort(pageAddr, off + 1, size);
                    PageUtils.putBytes(pageAddr, off + 3, s);
                    return s.length + 3;
                }
            }

            case IndexValueType.BYTES: {
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
    @SuppressWarnings("RedundantIfStatement")
    protected boolean canRelyOnCompare(int c, Value shortVal, Value v2) {
        switch (type) {
            case IndexValueType.STRING:
            case IndexValueType.STRING_FIXED:
            case IndexValueType.STRING_IGNORECASE:
            case IndexValueType.BYTES:
                if (shortVal.getType() == IndexValueType.NULL || v2.getType() == IndexValueType.NULL)
                    return true;

                if (c == 0 && shortVal.getType() != IndexValueType.NULL && v2.getType() != IndexValueType.NULL)
                    return false;

                int l1;
                int l2;

                if (type == IndexValueType.BYTES) {
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
     * @return Fixed compare result.
     */
    private int fixSort(int c) {
        return descending ? -c : c;
    }
}
