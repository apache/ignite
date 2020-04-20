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

package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueString;

/**
 * Inline index column implementation for inlining {@link String} values.
 */
public class StringInlineIndexColumn extends AbstractInlineIndexColumn {
    /** Default charset. */
    protected static final Charset CHARSET = StandardCharsets.UTF_8;

    /** Whether respect case or not while comparing. */
    private final boolean compareIgnoreCase;

    /** Whether to use optimized comparison or not. */
    private final boolean useOptimizedCompare;

    /**
     * @param col Column.
     */
    public StringInlineIndexColumn(Column col, boolean useOptimizedCompare) {
        this(col, Value.STRING, useOptimizedCompare, false);
    }

    /**
     * @param col Column.
     * @param type Type.
     */
    StringInlineIndexColumn(Column col, int type, boolean useOptimizedCompare, boolean compareIgnoreCase) {
        super(col, type, (short)-1);

        this.compareIgnoreCase = compareIgnoreCase;
        this.useOptimizedCompare = useOptimizedCompare;
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type || !useOptimizedCompare)
            return COMPARE_UNSUPPORTED;

        return compareAsString(pageAddr, off, v, compareIgnoreCase);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getType();

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

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueString.get(new String(readBytes(pageAddr, off), CHARSET));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType() == type();

        return val.getString().getBytes(CHARSET).length + 3;
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
                return Integer.signum(v1 - v2);
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
                        return Integer.signum(v1 - v2);

                    if (cntr2 == len2)
                        // The string is malformed (partial partial character at the end).
                        // Finish comparison here.
                        return 1;

                    // Low surrogate.
                    v1 = (char)(0xDC00 + (c & 0x3FF));
                    v2 = s.charAt(cntr2++);

                    if (v1 != v2)
                        return Integer.signum(v1 - v2);

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
                return Integer.signum(v1 - v2);
        }

        int res = cntr1 == len1 && cntr2 == len2 ? 0 : cntr1 == len1 ? -1 : 1;

        if (isValueFull(pageAddr, off))
            return res;

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return 1;

        return CANT_BE_COMPARE;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return {@code True} if string is not truncated on save.
     */
    private boolean isValueFull(long pageAddr, int off) {
        return (PageUtils.getShort(pageAddr, off + 1) & 0x8000) == 0;
    }

    /**
     * Convert String to byte[] with size limit, according to UTF-8 encoding.
     *
     * @param bytes byte[].
     * @param limit Size limit.
     * @return byte[].
     */
    static byte[] trimUTF8(byte[] bytes, int limit) {
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
}
