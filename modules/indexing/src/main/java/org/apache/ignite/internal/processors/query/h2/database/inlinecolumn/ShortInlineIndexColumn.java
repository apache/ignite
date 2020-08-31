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
import org.h2.value.ValueShort;

/**
 * Inline index column implementation for inlining {@link Short} values.
 */
public class ShortInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    public ShortInlineIndexColumn(Column col) {
        super(col, Value.SHORT, (short)2);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        short val1 = PageUtils.getShort(pageAddr, off + 1);
        short val2 = v.getShort();

        return Integer.signum(val1 - val2);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getType();

        PageUtils.putByte(pageAddr, off, (byte)val.getType());
        PageUtils.putShort(pageAddr, off + 1, val.getShort());

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueShort.get(PageUtils.getShort(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType() == type();

        return size() + 1;
    }
}
