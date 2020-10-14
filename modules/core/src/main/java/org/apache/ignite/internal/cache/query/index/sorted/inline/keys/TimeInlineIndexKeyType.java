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

import java.sql.Time;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link Time} values.
 */
public class TimeInlineIndexKeyType extends NullableInlineIndexKeyType<Time> {
    /** */
    public TimeInlineIndexKeyType() {
        super(IndexKeyTypes.TIME, (short)8);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Time v) {
        long val1 = PageUtils.getLong(pageAddr, off + 1);
        long val2 = v.getTime(); // TODO to nanons?

        return Integer.signum(Long.compare(val1, val2));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Time val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putLong(pageAddr, off + 1, val.getTime());

        return keySize() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Time get0(long pageAddr, int off) {
        return new Time(PageUtils.getLong(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Time val) {
        return keySize() + 1;
    }
}
