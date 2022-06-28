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
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimestampIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.MAX_DATE_VALUE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.MIN_DATE_VALUE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.NANOS_PER_DAY;

/**
 * Inline index key implementation for inlining {@link TimestampIndexKey} values.
 */
public class TimestampInlineIndexKeyType extends NullableInlineIndexKeyType<TimestampIndexKey> {
    /** */
    public TimestampInlineIndexKeyType() {
        super(IndexKeyTypes.TIMESTAMP, (short)16);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, TimestampIndexKey key) {
        long val1 = PageUtils.getLong(pageAddr, off + 1);

        int c = Long.compare(val1, key.dateValue());

        if (c != 0)
            return Integer.signum(c);

        long nanos1 = PageUtils.getLong(pageAddr, off + 9);

        return Integer.signum(Long.compare(nanos1, key.nanos()));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, TimestampIndexKey key, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte)type());

        PageUtils.putLong(pageAddr, off + 1, key.dateValue());
        PageUtils.putLong(pageAddr, off + 9, key.nanos());

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected TimestampIndexKey get0(long pageAddr, int off) {
        long dv = PageUtils.getLong(pageAddr, off + 1);
        long nanos = PageUtils.getLong(pageAddr, off + 9);

        if (dv > MAX_DATE_VALUE) {
            dv = MAX_DATE_VALUE;
            nanos = NANOS_PER_DAY - 1;
        }
        else if (dv < MIN_DATE_VALUE) {
            dv = MIN_DATE_VALUE;
            nanos = 0;
        }

        return new TimestampIndexKey(dv, nanos);
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(TimestampIndexKey key) {
        return keySize + 1;
    }
}
