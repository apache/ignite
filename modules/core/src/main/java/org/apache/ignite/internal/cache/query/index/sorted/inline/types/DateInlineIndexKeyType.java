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
import org.apache.ignite.internal.cache.query.index.sorted.keys.DateIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.MAX_DATE_VALUE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.MIN_DATE_VALUE;

/**
 * Inline index key implementation for inlining {@link DateIndexKey} values.
 */
public class DateInlineIndexKeyType extends NullableInlineIndexKeyType<DateIndexKey> {
    /** */
    public DateInlineIndexKeyType() {
        super(IndexKeyTypes.DATE, (short)8);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, DateIndexKey key) {
        long val1 = PageUtils.getLong(pageAddr, off + 1);
        long val2 = key.dateValue();

        return Integer.signum(Long.compare(val1, val2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, DateIndexKey key, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte)type());
        PageUtils.putLong(pageAddr, off + 1, key.dateValue());

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected DateIndexKey get0(long pageAddr, int off) {
        long dateVal = PageUtils.getLong(pageAddr, off + 1);

        if (dateVal > MAX_DATE_VALUE)
            dateVal = MAX_DATE_VALUE;
        else if (dateVal < MIN_DATE_VALUE)
            dateVal = MIN_DATE_VALUE;

        return new DateIndexKey(dateVal);
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(DateIndexKey key) {
        return keySize + 1;
    }
}
