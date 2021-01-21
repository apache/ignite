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

package org.apache.ignite.internal.cache.query.index.sorted.inline.keys;

import java.time.LocalDate;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.DateTimeUtils.MAX_DATE_VALUE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.DateTimeUtils.MIN_DATE_VALUE;

/**
 * Inline index key implementation for inlining {@link LocalDate} values.
 */
public class LocalDateInlineIndexKeyType extends NullableInlineIndexKeyType<LocalDate> {
    /** */
    public LocalDateInlineIndexKeyType() {
        super(IndexKeyTypes.DATE, (short)8);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, LocalDate v) {
        long val1 = PageUtils.getLong(pageAddr, off + 1);
        long val2 = DateTimeUtils.dateValue(v.getYear(), v.getMonthValue(), v.getDayOfMonth());

        return Integer.signum(Long.compare(val1, val2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, LocalDate val, int maxSize) {
        long dv = DateTimeUtils.dateValue(val.getYear(), val.getMonthValue(), val.getDayOfMonth());

        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putLong(pageAddr, off + 1, dv);

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected LocalDate get0(long pageAddr, int off) {
        long dateValue = PageUtils.getLong(pageAddr, off + 1);

        if (dateValue > MAX_DATE_VALUE)
            dateValue = MAX_DATE_VALUE;
        else if (dateValue < MIN_DATE_VALUE)
            dateValue = MIN_DATE_VALUE;

        return LocalDate.of(DateTimeUtils.yearFromDateValue(dateValue), DateTimeUtils.monthFromDateValue(dateValue),
            DateTimeUtils.dayFromDateValue(dateValue));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(LocalDate val) {
        return keySize + 1;
    }
}
