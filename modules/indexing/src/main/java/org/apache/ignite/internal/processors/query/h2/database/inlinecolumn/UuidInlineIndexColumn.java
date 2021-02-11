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

import java.util.UUID;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueUuid;

/**
 * Inline index column implementation for inlining {@link UUID} values.
 */
public class UuidInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    public UuidInlineIndexColumn(Column col) {
        super(col, Value.UUID, (short)16);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        if (type() != type)
            return COMPARE_UNSUPPORTED;

        ValueUuid uuid = (ValueUuid)v.convertTo(type);

        long part1 = PageUtils.getLong(pageAddr, off + 1);

        int c = Integer.signum(Long.compare(part1, uuid.getHigh()));

        if (c != 0)
            return c;

        long part2 = PageUtils.getLong(pageAddr, off + 9);

        return Integer.signum(Long.compare(part2, uuid.getLow()));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getType();

        PageUtils.putByte(pageAddr, off, (byte)val.getType());
        PageUtils.putLong(pageAddr, off + 1, ((ValueUuid)val).getHigh());
        PageUtils.putLong(pageAddr, off + 9, ((ValueUuid)val).getLow());

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        return ValueUuid.get(
            PageUtils.getLong(pageAddr, off + 1),
            PageUtils.getLong(pageAddr, off + 9)
        );
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType() == type();

        return size() + 1;
    }
}
