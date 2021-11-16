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
import org.apache.ignite.internal.cache.query.index.sorted.keys.DoubleIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link Double} values.
 */
public class DoubleInlineIndexKeyType extends NullableInlineIndexKeyType<DoubleIndexKey> {
    /** */
    public DoubleInlineIndexKeyType() {
        super(IndexKeyTypes.DOUBLE, (short)8);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, DoubleIndexKey v) {
        double val1 = Double.longBitsToDouble(PageUtils.getLong(pageAddr, off + 1));

        return Integer.signum(Double.compare(val1, (double)v.key()));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, DoubleIndexKey key, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte)type());
        PageUtils.putLong(pageAddr, off + 1, Double.doubleToLongBits((double)key.key()));

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected DoubleIndexKey get0(long pageAddr, int off) {
        double key = Double.longBitsToDouble(PageUtils.getLong(pageAddr, off + 1));

        return new DoubleIndexKey(key);
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(DoubleIndexKey val) {
        return keySize + 1;
    }
}
