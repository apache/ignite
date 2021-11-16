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
import org.apache.ignite.internal.cache.query.index.sorted.keys.LongIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index column implementation for inlining {@link Integer} values.
 */
public class LongInlineIndexKeyType extends NullableInlineIndexKeyType<LongIndexKey> {
    /** Constructor. */
    public LongInlineIndexKeyType() {
        super(IndexKeyTypes.LONG, (short)8);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, LongIndexKey key, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte)type());
        // +1 shift after type
        PageUtils.putLong(pageAddr, off + 1, (long)key.key());

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected LongIndexKey get0(long pageAddr, int off) {
        // +1 shift after type
        long key = PageUtils.getLong(pageAddr, off + 1);

        return new LongIndexKey(key);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, LongIndexKey key) {
        long val1 = PageUtils.getLong(pageAddr, off + 1);

        return Integer.signum(Long.compare(val1, (long)key.key()));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(LongIndexKey key) {
        return keySize + 1;
    }
}
