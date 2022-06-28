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

package org.apache.ignite.internal.cache.query.index.sorted.inline.types;

import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract inline key. Store base logic for work with inlined keys. Handle NULL values.
 */
public abstract class NullableInlineIndexKeyType<T extends IndexKey> implements InlineIndexKeyType {
    /** Value for comparison meaning 'Not enough information to compare'. */
    public static final int CANT_BE_COMPARE = -2;

    /** Value for comparison meaning 'Compare not supported for given value'. */
    public static final int COMPARE_UNSUPPORTED = Integer.MIN_VALUE;

    /** Type of this key. */
    private final int type;

    /** Actual size of a key without type field. */
    protected final short keySize;

    /**
     * @param type Index key type.
     * @param keySize Size of value stored in the key.
     */
    protected NullableInlineIndexKeyType(int type, short keySize) {
        this.type = type;
        this.keySize = keySize;
    }

    /** {@inheritDoc} */
    @Override public int type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize(long pageAddr, int off) {
        int type = PageUtils.getByte(pageAddr, off);

        if (type == IndexKeyTypes.NULL)
            return 1;

        if (keySize > 0)
            // For fixed length types.
            return keySize + 1;
        else
            // For variable length types.
            return (PageUtils.getShort(pageAddr, off + 1) & 0x7FFF) + 3;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        if (type == IndexKeyTypes.NULL)
            return 1;

        // For variable length keys returns -1.
        return keySize < 0 ? keySize : keySize + 1;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize(IndexKey key) {
        if (key == NullIndexKey.INSTANCE)
            return 1;

        ensureKeyType(key);

        return inlineSize0((T)key);
    }

    /**
     * Restores value from inline, if possible.
     *
     * @param pageAddr Address of the page.
     * @param off Offset on the page.
     * @param maxSize Max size to read.
     *
     * @return Restored value or {@code null} if value can't be restored.
     */
    @Override public IndexKey get(long pageAddr, int off, int maxSize) {
        if (keySize > 0 && keySize + 1 > maxSize)
            return null;

        if (maxSize < 1)
            return null;

        int type = PageUtils.getByte(pageAddr, off);

        if (type == IndexKeyTypes.UNKNOWN)
            return null;

        if (type == IndexKeyTypes.NULL)
            return NullIndexKey.INSTANCE;

        ensureKeyType(type);

        IndexKey o = get0(pageAddr, off);

        if (o == null)
            return NullIndexKey.INSTANCE;

        return o;
    }

    /** {@inheritDoc} */
    @Override public int put(long pageAddr, int off, IndexKey key, int maxSize) {
        // +1 is a length of the type byte.
        if (keySize > 0 && keySize + 1 > maxSize)
            return 0;

        if (keySize < 0 && maxSize < 4) {
            // Can't fit vartype field.
            PageUtils.putByte(pageAddr, off, (byte)IndexKeyTypes.UNKNOWN);
            return 0;
        }

        if (key == NullIndexKey.INSTANCE) {
            PageUtils.putByte(pageAddr, off, (byte)IndexKeyTypes.NULL);
            return 1;
        }

        ensureKeyType(key);

        return put0(pageAddr, off, (T)key, maxSize);
    }

    /** {@inheritDoc} */
    @Override public short keySize() {
        return keySize;
    }

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
    protected abstract int put0(long pageAddr, int off, T val, int maxSize);

    /**
     * Restores value from inline.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     *
     * @return Inline value or {@code null} if value can't be restored.
     */
    protected abstract @Nullable T get0(long pageAddr, int off);

    /** Read variable length bytearray */
    public static byte[] readBytes(long pageAddr, int off) {
        int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
        return PageUtils.getBytes(pageAddr, off + 3, size);
    }

    /** {@inheritDoc} */
    @Override public int compare(long pageAddr, int off, int maxSize, IndexKey key) {
        int type;

        if ((keySize > 0 && keySize + 1 > maxSize)
            || maxSize < 1
            || (type = PageUtils.getByte(pageAddr, off)) == (byte)IndexKeyTypes.UNKNOWN)
            return CANT_BE_COMPARE;

        if (type == IndexKeyTypes.NULL) {
            if (key == NullIndexKey.INSTANCE)
                return 0;
            else
                return -1;
        }

        if (type() != type)
            return COMPARE_UNSUPPORTED;

        if (key == NullIndexKey.INSTANCE)
            return 1;

        return compare0(pageAddr, off, (T)key);
    }

    /**
     * Checks whether specified val corresponds to this key type.
     */
    private void ensureKeyType(int type) {
        if (this.type != type)
            throw new UnsupportedOperationException("Value type doesn't match: exp=" + this.type + ", act=" + type);
    }

    /**
     * Checks whether specified val corresponds to this key type.
     */
    private void ensureKeyType(IndexKey key) {
        if (key != NullIndexKey.INSTANCE && type != key.type())
            throw new UnsupportedOperationException(key.type() + " cannot be used for inline type " + type());
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
    public abstract int compare0(long pageAddr, int off, T v);

    /** Return inlined size for specified key. */
    protected abstract int inlineSize0(T key);
}
