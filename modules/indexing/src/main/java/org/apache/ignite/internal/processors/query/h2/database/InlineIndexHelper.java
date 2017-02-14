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
import java.util.List;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueString;

/**
 * Helper class for in-page indexes.
 */
public class InlineIndexHelper {
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    /** */
    public static final List<Integer> AVAILABLE_TYPES = Arrays.asList(
        Value.BOOLEAN,
        Value.BYTE,
        Value.SHORT,
        Value.INT,
        Value.LONG,
        Value.STRING
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
     * @return Value size.
     */
    public short size() {
        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
                return 1;

            case Value.INT:
                return 4;

            case Value.SHORT:
                return 2;

            case Value.LONG:
                return 8;

            case Value.STRING:
                return -1;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Size for variable-length field.
     */
    public short readSize(long pageAddr, int off) {
        switch (type) {
            case Value.STRING:
                return PageUtils.getShort(pageAddr, off);
            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Value.
     */
    public Value get(long pageAddr, int off, int maxSize) {
        if (size() > 0 && size() > maxSize)
            return null;

        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get(PageUtils.getByte(pageAddr, off) != 0);

            case Value.BYTE:
                return ValueByte.get(PageUtils.getByte(pageAddr, off));

            case Value.SHORT:
                return ValueInt.get(PageUtils.getShort(pageAddr, off));

            case Value.INT:
                return ValueInt.get(PageUtils.getInt(pageAddr, off));

            case Value.LONG:
                return ValueLong.get(PageUtils.getLong(pageAddr, off));

            case Value.STRING:
                short size = readSize(pageAddr, off);
                return ValueString.get(new String(PageUtils.getBytes(pageAddr, off + 2, size), CHARSET));

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
        if (size() > 0 && size() >= maxSize)
            return 0;

        switch (type) {
            case Value.BOOLEAN:
                PageUtils.putByte(pageAddr, off, (byte)(val.getBoolean() ? 1 : 0));
                return size();

            case Value.BYTE:
                PageUtils.putByte(pageAddr, off, val.getByte());
                return size();

            case Value.SHORT:
                PageUtils.putShort(pageAddr, off, val.getShort());
                return size();

            case Value.INT:
                PageUtils.putInt(pageAddr, off, val.getInt());
                return size();

            case Value.LONG:
                PageUtils.putLong(pageAddr, off, val.getLong());
                return size();

            case Value.STRING:
                byte[] s;
                if (val.getString().getBytes().length + 2 <= maxSize)
                    s = val.getString().getBytes(CHARSET);
                else
                    s = toBytes(val.getString(), maxSize - 2);
                PageUtils.putShort(pageAddr, off, (short)s.length);
                PageUtils.putBytes(pageAddr, off + 2, s);
                return s.length + 2;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /**
     * Convert String to byte[] with size limit, according to UTF-8 encoding.
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
}
