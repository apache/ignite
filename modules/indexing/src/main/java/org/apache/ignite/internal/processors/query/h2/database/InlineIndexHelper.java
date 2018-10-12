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
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
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
    /** Value for comparison meaning 'Not enough information to compare'. */
    public static final int CANT_BE_COMPARE = -2;

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
        Value.FLOAT,
        Value.DOUBLE,
        Value.DATE,
        Value.TIME,
        Value.TIMESTAMP,
        Value.UUID,
        Value.STRING,
        Value.STRING_FIXED,
        Value.STRING_IGNORECASE,
        Value.BYTES,
        Value.JAVA_OBJECT
    );

    /** */
    private final String colName;

    /** */
    private final int type;

    /** */
    private final int colIdx;

    /** */
    private final int sortType;

    /** */
    private final short size;

    /** */
    private final boolean compareBinaryUnsigned;

    /** */
    private final boolean compareStringsOptimized;

    /**
     * @param colName Column name.
     * @param type Index type (see {@link Value}).
     * @param colIdx Index column index.
     * @param sortType Column sort type (see {@link IndexColumn#sortType}).
     * @param compareMode Compare mode.
     */
    public InlineIndexHelper(String colName, int type, int colIdx, int sortType,
        CompareMode compareMode) {

        this.colName = colName;
        this.type = type;
        this.colIdx = colIdx;
        this.sortType = sortType;

        this.compareBinaryUnsigned = compareMode.isBinaryUnsigned();

        // Optimized strings comparison can be used only if there are no custom collators.
        // H2 internal comparison will be used otherwise (may be slower).
        this.compareStringsOptimized = CompareMode.OFF.equals(compareMode.getName());

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
            case Value.JAVA_OBJECT:
                this.size = -1;
                break;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * @return Column name
     */
    public String colName() {
        return colName;
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

            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(null, readBytes(pageAddr, off), null);

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
            case Value.JAVA_OBJECT:
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
     * @return Compare result ( {@code CANT_BE_COMPARE} means we can't compare).
     */
    public int compare(long pageAddr, int off, int maxSize, Value v, Comparator<Value> comp) {
        int c = tryCompareOptimized(pageAddr, off, maxSize, v);

        if (c != Integer.MIN_VALUE)
            return c;

        Value v1 = get(pageAddr, off, maxSize);

        if (v1 == null)
            return CANT_BE_COMPARE;

        c = Integer.signum(comp.compare(v1, v));

        if (size > 0)
            return fixSort(c, sortType());

        if (isValueFull(pageAddr, off) || canRelyOnCompare(c, v1, v))
            return fixSort(c, sortType());

        return CANT_BE_COMPARE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param maxSize Maximum size to read.
     * @param v Value to compare.
     * @return Compare result ({@code Integer.MIN_VALUE} means unsupported operation; {@code CANT_BE_COMPARE} - can't compare).
     */
    private int tryCompareOptimized(long pageAddr, int off, int maxSize, Value v) {
        int type;

        if ((size > 0 && size + 1 > maxSize)
                || maxSize < 1
                || (type = PageUtils.getByte(pageAddr, off)) == Value.UNKNOWN)
            return CANT_BE_COMPARE;

        if (type == Value.NULL)
            return Integer.MIN_VALUE;

        if (v == ValueNull.INSTANCE)
            return fixSort(1, sortType());

        if (this.type != type)
            throw new UnsupportedOperationException("Invalid fast index type: " + type);

        type = Value.getHigherOrder(type, v.getType());

        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
            case Value.SHORT:
            case Value.INT:
            case Value.LONG:
            case Value.FLOAT:
            case Value.DOUBLE:
                return compareAsPrimitive(pageAddr, off, v, type);

            case Value.TIME:
            case Value.DATE:
            case Value.TIMESTAMP:
                return compareAsDateTime(pageAddr, off, v, type);

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                if (compareStringsOptimized)
                    return compareAsString(pageAddr, off, v, type == Value.STRING_IGNORECASE);

                break;

            case Value.UUID:
                return compareAsUUID(pageAddr, off, v, type);

            case Value.BYTES:
            case Value.JAVA_OBJECT:
                return compareAsBytes(pageAddr, off, v);
        }

        return Integer.MIN_VALUE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param v Value to compare.
     * @param type Highest value type.
     * @return Compare result ({@code Integer.MIN_VALUE} means unsupported operation.
     */
    private int compareAsUUID(long pageAddr, int off, Value v, int type) {
        // only compatible types are supported now.
        if(PageUtils.getByte(pageAddr, off) == type) {
            assert type == Value.UUID;

            ValueUuid uuid = (ValueUuid)v.convertTo(Value.UUID);
            long long1 = PageUtils.getLong(pageAddr, off + 1);

            int c = Long.compare(long1, uuid.getHigh());

            if(c != 0)
                return fixSort(c, sortType());

            long1 = PageUtils.getLong(pageAddr, off + 9);

            c = Long.compare(long1, uuid.getLow());

            return fixSort(c, sortType());
        }

        return Integer.MIN_VALUE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param v Value to compare.
     * @param type Highest value type.
     * @return Compare result ({@code Integer.MIN_VALUE} means unsupported operation.
     */
    private int compareAsDateTime(long pageAddr, int off, Value v, int type) {
        // only compatible types are supported now.
        if(PageUtils.getByte(pageAddr, off) == type) {
            switch (type) {
                case Value.TIME:
                    long nanos1 = PageUtils.getLong(pageAddr, off + 1);
                    long nanos2 = ((ValueTime)v.convertTo(type)).getNanos();

                    return fixSort(Long.signum(nanos1 - nanos2), sortType());

                case Value.DATE:
                    long date1 = PageUtils.getLong(pageAddr, off + 1);
                    long date2 = ((ValueDate)v.convertTo(type)).getDateValue();

                    return fixSort(Long.signum(date1 - date2), sortType());

                case Value.TIMESTAMP:
                    ValueTimestamp v0 = (ValueTimestamp) v.convertTo(type);

                    date1 = PageUtils.getLong(pageAddr, off + 1);
                    date2 = v0.getDateValue();

                    int c = Long.signum(date1 - date2);

                    if (c == 0) {
                        nanos1 = PageUtils.getLong(pageAddr, off + 9);
                        nanos2 = v0.getTimeNanos();

                        c = Long.signum(nanos1 - nanos2);
                    }

                    return fixSort(c, sortType());
            }
        }

        return Integer.MIN_VALUE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param v Value to compare.
     * @param type Highest value type.
     * @return Compare result ({@code Integer.MIN_VALUE} means unsupported operation.
     */
    private int compareAsPrimitive(long pageAddr, int off, Value v, int type) {
        // only compatible types are supported now.
        if(PageUtils.getByte(pageAddr, off) == type) {
            switch (type) {
                case Value.BOOLEAN:
                    boolean bool1 = PageUtils.getByte(pageAddr, off + 1) != 0;
                    boolean bool2 = v.getBoolean();

                    return fixSort(Boolean.compare(bool1, bool2), sortType());

                case Value.BYTE:
                    byte byte1 = PageUtils.getByte(pageAddr, off + 1);
                    byte byte2 = v.getByte();

                    return fixSort(Integer.signum(byte1 - byte2), sortType());

                case Value.SHORT:
                    short short1 = PageUtils.getShort(pageAddr, off + 1);
                    short short2 = v.getShort();

                    return fixSort(Integer.signum(short1 - short2), sortType());

                case Value.INT:
                    int int1 = PageUtils.getInt(pageAddr, off + 1);
                    int int2 = v.getInt();

                    return fixSort(Integer.compare(int1, int2), sortType());

                case Value.LONG:
                    long long1 = PageUtils.getLong(pageAddr, off + 1);
                    long long2 = v.getLong();

                    return fixSort(Long.compare(long1, long2), sortType());

                case Value.FLOAT:
                    float float1 = Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1));
                    float float2 = v.getFloat();

                    return fixSort(Float.compare(float1, float2), sortType());

                case Value.DOUBLE:
                    double double1 = Double.longBitsToDouble(PageUtils.getLong(pageAddr, off + 1));
                    double double2 = v.getDouble();

                    return fixSort(Double.compare(double1, double2), sortType());
            }
        }

        return Integer.MIN_VALUE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param v Value to compare.
     * @return Compare result ({@code CANT_BE_COMPARE} means we can't compare).
     */
    private int compareAsBytes(long pageAddr, int off, Value v) {
        byte[] bytes = v.getBytesNoCopy();

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
                    return fixSort(Integer.signum(b1 - b2), sortType());
            }
        }
        else {
            for (int i = 0; i < len; i++) {
                byte b1 = GridUnsafe.getByte(addr + i);
                byte b2 = bytes[i];

                if (b1 != b2)
                    return fixSort(Integer.signum(b1 - b2), sortType());
            }
        }

        int res = Integer.signum(len1 - len2);

        if(isValueFull(pageAddr, off))
            return fixSort(res, sortType());

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return fixSort(1, sortType());

        return CANT_BE_COMPARE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @param v Value to compare.
     * @param ignoreCase {@code True} if a case-insensitive comparison should be used.
     * @return Compare result ({@code CANT_BE_COMPARE} means we can't compare).
     */
    private int compareAsString(long pageAddr, int off, Value v, boolean ignoreCase) {
        String s = v.getString();

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
                return fixSort(Integer.signum(v1 - v2), sortType());
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
                        return fixSort(Integer.signum(v1 - v2), sortType());

                    if (cntr2 == len2)
                        // The string is malformed (partial partial character at the end).
                        // Finish comparison here.
                        return fixSort(1, sortType());

                    // Low surrogate.
                    v1 = (char)(0xDC00 + (c & 0x3FF));
                    v2 = s.charAt(cntr2++);

                    if (v1 != v2)
                        return fixSort(Integer.signum(v1 - v2), sortType());

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
                return fixSort(Integer.signum(v1 - v2), sortType());
        }

        int res = cntr1 == len1 && cntr2 == len2 ? 0 : cntr1 == len1 ? -1 : 1;

        if (isValueFull(pageAddr, off))
            return fixSort(res, sortType());

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return fixSort(1, sortType());

        return CANT_BE_COMPARE;
    }

    /**
     * Calculate size to inline given value.
     *
     * @param val Value to calculate inline size.
     * @return Calculated inline size for given value.
     */
    public int inlineSizeOf(Value val){
        if (val.getType() == Value.NULL)
            return 1;

        if (val.getType() != type)
            throw new UnsupportedOperationException("value type doesn't match");

        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
            case Value.SHORT:
            case Value.INT:
            case Value.LONG:
            case Value.FLOAT:
            case Value.DOUBLE:
            case Value.TIME:
            case Value.DATE:
            case Value.TIMESTAMP:
            case Value.UUID:
                return size + 1;

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                return val.getString().getBytes(CHARSET).length + 3;

            case Value.BYTES:
            case Value.JAVA_OBJECT:
                return val.getBytes().length + 3;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
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
                    size = (short)(s == null ? 0 : s.length | 0x8000);
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

            case Value.BYTES:
            case Value.JAVA_OBJECT:
             {
                short size;

                PageUtils.putByte(pageAddr, off, (byte)val.getType());

                 byte[] bytes = val.getBytes();

                 if (bytes.length + 3 <= maxSize) {
                    size = (short)bytes.length;
                    PageUtils.putShort(pageAddr, off + 1, size);
                    PageUtils.putBytes(pageAddr, off + 3, bytes);

                    return size + 3;
                }
                else {
                    size = (short)((maxSize - 3) | 0x8000);
                    PageUtils.putShort(pageAddr, off + 1, size);
                    PageUtils.putBytes(pageAddr, off + 3, Arrays.copyOfRange(bytes, 0, maxSize - 3));

                    return maxSize;
                }
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
            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
            case Value.BYTES:
            case Value.JAVA_OBJECT:
                if (shortVal.getType() == Value.NULL || v2.getType() == Value.NULL)
                    return true;

                if (c == 0 && shortVal.getType() != Value.NULL && v2.getType() != Value.NULL)
                    return false;

                int l1;
                int l2;

                if (type == Value.BYTES || type == Value.JAVA_OBJECT) {
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

    /**
     * @param typeCode Type code.
     * @return Name.
     */
    public static String nameTypeBycode(int typeCode) {
        switch (typeCode) {
            case Value.UNKNOWN:
                return "UNKNOWN";
            case Value.NULL:
                return "NULL";
            case Value.BOOLEAN:
                return "BOOLEAN";
            case Value.BYTE:
                return "BYTE";
            case Value.SHORT:
                return "SHORT";
            case Value.INT:
                return "INT";
            case Value.LONG:
                return "LONG";
            case Value.DECIMAL:
                return "DECIMAL";
            case Value.DOUBLE:
                return "DOUBLE";
            case Value.FLOAT:
                return "FLOAT";
            case Value.TIME:
                return "TIME";
            case Value.DATE:
                return "DATE";
            case Value.TIMESTAMP:
                return "TIMESTAMP";
            case Value.BYTES:
                return "BYTES";
            case Value.STRING:
                return "STRING";
            case Value.STRING_IGNORECASE:
                return "STRING_IGNORECASE";
            case Value.BLOB:
                return "BLOB";
            case Value.CLOB:
                return "CLOB";
            case Value.ARRAY:
                return "ARRAY";
            case Value.RESULT_SET:
                return "RESULT_SET";
            case Value.JAVA_OBJECT:
                return "JAVA_OBJECT";
            case Value.UUID:
                return "UUID";
            case Value.STRING_FIXED:
                return "STRING_FIXED";
            case Value.GEOMETRY:
                return "GEOMETRY";
            case Value.TIMESTAMP_TZ:
                return "TIMESTAMP_TZ";
            case Value.ENUM:
                return "ENUM";
            default:
                return "UNKNOWN type " + typeCode;
        }
    }
}
