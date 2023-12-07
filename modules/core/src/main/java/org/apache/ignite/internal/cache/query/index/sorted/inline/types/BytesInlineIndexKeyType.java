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

import java.util.Arrays;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.BytesIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.SignedBytesIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Inline index key implementation for inlining byte arrays.
 */
public class BytesInlineIndexKeyType extends NullableInlineIndexKeyType<BytesIndexKey> {
    /** Compare binary unsigned. */
    private final boolean compareBinaryUnsigned;

    /** */
    public BytesInlineIndexKeyType() {
        this(IndexKeyType.BYTES);
    }

    /** */
    public BytesInlineIndexKeyType(IndexKeyType type) {
        this(type, true);
    }

    /** */
    public BytesInlineIndexKeyType(boolean compareBinaryUnsigned) {
        this(IndexKeyType.BYTES, compareBinaryUnsigned);
    }

    /** */
    public BytesInlineIndexKeyType(IndexKeyType type, boolean compareBinaryUnsigned) {
        super(type, (short)-1);

        this.compareBinaryUnsigned = compareBinaryUnsigned;
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, IndexKey bytes) {
        long addr = pageAddr + off + 1; // Skip type.

        int len1 = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;

        addr += 2; // Skip size.

        byte[] arr = (byte[])bytes.key();

        int len2 = arr.length;

        int len = Math.min(len1, len2);

        if (compareBinaryUnsigned) {
            for (int i = 0; i < len; i++) {
                int b1 = GridUnsafe.getByte(addr + i) & 0xff;
                int b2 = arr[i] & 0xff;

                if (b1 != b2)
                    return Integer.signum(b1 - b2);
            }
        }
        else {
            for (int i = 0; i < len; i++) {
                byte b1 = GridUnsafe.getByte(addr + i);
                byte b2 = arr[i];

                if (b1 != b2)
                    return Integer.signum(b1 - b2);
            }
        }

        int res = Integer.signum(len1 - len2);

        if (inlinedFullValue(pageAddr, off, VARTYPE_HEADER_SIZE + 1))
            return res;

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return 1;

        return CANT_BE_COMPARE;
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, BytesIndexKey key, int maxSize) {
        short size;

        PageUtils.putByte(pageAddr, off, (byte)type().code());

        byte[] val = (byte[])key.key();

        if (val.length + 3 <= maxSize) {
            size = (short)val.length;
            PageUtils.putShort(pageAddr, off + 1, size);
            PageUtils.putBytes(pageAddr, off + 3, val);

            return size + 3;
        }
        else {
            size = (short)((maxSize - 3) | 0x8000);
            PageUtils.putShort(pageAddr, off + 1, size);
            PageUtils.putBytes(pageAddr, off + 3, Arrays.copyOfRange(val, 0, maxSize - 3));

            return maxSize;
        }
    }

    /** {@inheritDoc} */
    @Override protected BytesIndexKey get0(long pageAddr, int off) {
        byte[] arr = readBytes(pageAddr, off);

        return compareBinaryUnsigned ? new BytesIndexKey(arr) : new SignedBytesIndexKey(arr);
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(BytesIndexKey val) {
        byte[] arr = (byte[])val.key();

        return arr.length + 3;
    }

    /** */
    public boolean compareBinaryUnsigned() {
        return compareBinaryUnsigned;
    }
}
