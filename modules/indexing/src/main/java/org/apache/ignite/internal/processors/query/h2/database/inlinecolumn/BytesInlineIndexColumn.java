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

import java.util.Arrays;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueBytes;

/**
 * Inline index column implementation for inlining byte arrays.
 */
public class BytesInlineIndexColumn extends AbstractInlineIndexColumn {
    /** Compare binary unsigned. */
    private final boolean compareBinaryUnsigned;

    /**
     * @param col Column.
     */
    public BytesInlineIndexColumn(Column col, boolean compareBinaryUnsigned) {
        this(col, Value.BYTES, compareBinaryUnsigned);
    }

    /**
     * @param col Column.
     * @param type Type.
     * @param compareBinaryUnsigned Compare binary unsigned.
     */
    BytesInlineIndexColumn(Column col, int type, boolean compareBinaryUnsigned) {
        super(col, type, (short)-1);

        this.compareBinaryUnsigned = compareBinaryUnsigned;
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        byte[] bytes = v.getBytesNoCopy();

        long addr = pageAddr + off + 1; // Skip type.

        int len1 = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;

        addr += 2; // Skip size.

        int len2 = bytes.length;

        int len = Math.min(len1, len2);

        if (compareBinaryUnsigned) {
            for (int i = 0; i < len; i++) {
                int b1 = GridUnsafe.getByte(addr + i) & 0xff;
                int b2 = bytes[i] & 0xff;

                if (b1 != b2)
                    return Integer.signum(b1 - b2);
            }
        }
        else {
            for (int i = 0; i < len; i++) {
                byte b1 = GridUnsafe.getByte(addr + i);
                byte b2 = bytes[i];

                if (b1 != b2)
                    return Integer.signum(b1 - b2);
            }
        }

        int res = Integer.signum(len1 - len2);

        if (isValueFull(pageAddr, off))
            return res;

        if (res >= 0)
            // There are two cases:
            // a) The values are equal but the stored value is truncated, so that it's bigger.
            // b) Even truncated current value is longer, so that it's bigger.
            return 1;

        return CANT_BE_COMPARE;
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getType();

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

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueBytes.get(readBytes(pageAddr, off));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType() == type();

        return val.getBytes().length + 3;
    }

    /**
     * @param pageAddr Page address.
     * @param off Offset.
     * @return {@code True} if string is not truncated on save.
     */
    private boolean isValueFull(long pageAddr, int off) {
        return (PageUtils.getShort(pageAddr, off + 1) & 0x8000) == 0;
    }
}
