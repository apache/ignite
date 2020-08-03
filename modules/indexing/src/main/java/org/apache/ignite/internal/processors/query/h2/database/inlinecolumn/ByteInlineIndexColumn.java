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

import org.apache.ignite.internal.pagemem.PageUtils;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueByte;

/**
 * Inline index column implementation for inlining {@link Byte} values.
 */
public class ByteInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    ByteInlineIndexColumn(Column col) {
        super(col, Value.BYTE, (short)1);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        byte byte1 = PageUtils.getByte(pageAddr, off + 1);
        byte byte2 = v.getByte();

        return Integer.signum(byte1 - byte2);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getType();

        PageUtils.putByte(pageAddr, off, (byte)val.getType());
        PageUtils.putByte(pageAddr, off + 1, val.getByte());

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueByte.get(PageUtils.getByte(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType() == type();

        return size() + 1;
    }
}
