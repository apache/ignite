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

import java.util.Comparator;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexColumn;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract inline column.
 * Implements basic methods of interface {@link InlineIndexColumn}.
 */
public abstract class AbstractInlineIndexColumn implements InlineIndexColumn {
    /** Value for comparison meaning 'Not enough information to compare'. */
    public static final int CANT_BE_COMPARE = -2;

    /** Value for comparison meaning 'Compare not supported for given value'. */
    public static final int COMPARE_UNSUPPORTED = Integer.MIN_VALUE;

    /** */
    private final Column col;

    /** */
    private final int type;

    /** */
    private final short size;

    /**
     * @param col Column.
     * @param type Index type (see {@link Value}).
     * @param size Size.
     */
    protected AbstractInlineIndexColumn(Column col, int type, short size) {
        assert col.getType() == type : "columnType=" + col.getType() + ", type=" + type;

        this.col = col;
        this.type = type;
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public String columnName() {
        return col.getName();
    }

    /** {@inheritDoc} */
    @Override public int type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public int columnIndex() {
        return col.getColumnId();
    }

    /** {@inheritDoc} */
    @Override public short size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public int fullSize(long pageAddr, int off) {
        int type = PageUtils.getByte(pageAddr, off);

        if (type == Value.NULL)
            return 1;

        if (size > 0)
            return size + 1;
        else
            return (PageUtils.getShort(pageAddr, off + 1) & 0x7FFF) + 3;
    }

    /**
     * Restores value from inline, if possible.
     *
     * @param pageAddr Address of the page.
     * @param off Offset on the page.
     * @param maxSize Maxim size.
     *
     * @return Restored value or {@code null} if value can't be restored.
     */
    @Nullable Value get(long pageAddr, int off, int maxSize) {
        if (size > 0 && size + 1 > maxSize)
            return null;

        if (maxSize < 1)
            return null;

        int type = PageUtils.getByte(pageAddr, off);

        if (type == Value.UNKNOWN)
            return null;

        if (type == Value.NULL)
            return ValueNull.INSTANCE;

        ensureValueType(type);

        return get0(pageAddr, off);
    }

    /** {@inheritDoc} */
    @Override public int compare(long pageAddr, int off, int maxSize, Value v, Comparator<Value> comp) {
        int type;

        if ((size > 0 && size + 1 > maxSize)
            || maxSize < 1
            || (type = PageUtils.getByte(pageAddr, off)) == Value.UNKNOWN)
            return CANT_BE_COMPARE;

        if (type == Value.NULL)
            return Integer.signum(comp.compare(ValueNull.INSTANCE, v));

        if (v == ValueNull.INSTANCE)
            return 1;

        ensureValueType(type);

        type = Value.getHigherOrder(type, v.getType());

        int c = compare0(pageAddr, off, v, type);

        if (c != COMPARE_UNSUPPORTED)
            return c;

        Value v1 = get(pageAddr, off, maxSize);

        if (v1 == null)
            return CANT_BE_COMPARE;

        return Integer.signum(comp.compare(v1, v));
    }

    /** {@inheritDoc} */
    @Override public int inlineSizeOf(Value val) {
        if (val.getType() == Value.NULL)
            return 1;

        ensureValueType(val.getType());

        return inlineSizeOf0(val);
    }

    /** {@inheritDoc} */
    @Override public int put(long pageAddr, int off, Value val, int maxSize) {
        if (size > 0 && size + 1 > maxSize)
            return 0;

        if (size < 0 && maxSize < 4) {
            // can't fit vartype field
            PageUtils.putByte(pageAddr, off, (byte)Value.UNKNOWN);
            return 0;
        }

        int valType = val.getType();

        if (valType == Value.NULL) {
            PageUtils.putByte(pageAddr, off, (byte)Value.NULL);
            return 1;
        }

        ensureValueType(valType);

        return put0(pageAddr, off, val, maxSize);
    }

    /**
     * @param valType Value type.
     */
    private void ensureValueType(int valType) {
        if (valType != type)
            throw new UnsupportedOperationException("Value type doesn't match: exp=" + type + ", act=" + valType);
    }

    /**
     * Compares inlined and given value.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @param v Value that should be compare.
     *
     * @return -1, 0 or 1 if inlined value less, equal or greater
     * than given respectively, {@link #CANT_BE_COMPARE} if inlined part
     * is not enough to compare, or {@link #COMPARE_UNSUPPORTED} if given value
     * can't be compared with inlined part at all.
     */
    protected abstract int compare0(long pageAddr, int off, Value v, int type);

    /**
     * Puts given value into inline index tree.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @param val Value.
     * @param maxSize Max size.
     *
     * @return Amount of bytes actually stored.
     */
    protected abstract int put0(long pageAddr, int off, Value val, int maxSize);

    /**
     * Restores value from inline.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     *
     * @return Inline value or {@code null} if value can't be restored.
     */
    protected abstract @Nullable Value get0(long pageAddr, int off);

    /**
     * Calculate size required to inline given value.
     *
     * @param val Value to calculate inline size.
     *
     * @return Calculated inline size.
     */
    protected abstract int inlineSizeOf0(Value val);

    /** Read variable length bytearray */
    protected byte[] readBytes(long pageAddr, int off) {
        int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
        return PageUtils.getBytes(pageAddr, off + 3, size);
    }
}
