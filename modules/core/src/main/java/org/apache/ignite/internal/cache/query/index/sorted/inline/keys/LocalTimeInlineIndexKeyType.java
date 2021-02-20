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

import java.time.LocalTime;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link LocalTime} values.
 */
public class LocalTimeInlineIndexKeyType extends NullableInlineIndexKeyType<LocalTime> {
    /** */
    public LocalTimeInlineIndexKeyType() {
        super(IndexKeyTypes.TIME, (short)8);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, LocalTime v) {
        long val1 = PageUtils.getLong(pageAddr, off + 1);
        long val2 = get(v);

        return Integer.signum(Long.compare(val1, val2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, LocalTime val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putLong(pageAddr, off + 1, get(val));

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected LocalTime get0(long pageAddr, int off) {
        return LocalTime.ofNanoOfDay(PageUtils.getLong(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(LocalTime val) {
        return keySize + 1;
    }

    /**
     * Create nanos value for the given time.
     *
     * @param time Time.
     * @return Nanos.
     */
    public static long get(LocalTime time) {
        return DateTimeUtils.nanosFromDate(time.toNanoOfDay());
    }
}
