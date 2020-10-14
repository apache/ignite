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

import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link Short} values.
 */
public class ShortInlineIndexKeyType extends NullableInlineIndexKeyType<Short> {
    /** */
    public ShortInlineIndexKeyType() {
        super(IndexKeyTypes.SHORT, (short)2);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Short v) {
        short val1 = PageUtils.getShort(pageAddr, off + 1);

        return Integer.signum(val1 - v);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Short val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putShort(pageAddr, off + 1, val);

        return keySize() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Short get0(long pageAddr, int off) {
        return PageUtils.getShort(pageAddr, off + 1);
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Short val) {
        return keySize() + 1;
    }
}
