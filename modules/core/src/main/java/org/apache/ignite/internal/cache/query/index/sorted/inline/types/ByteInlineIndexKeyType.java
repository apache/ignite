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

import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ByteIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NumericIndexKey;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link Byte} values.
 */
public class ByteInlineIndexKeyType extends NumericInlineIndexKeyType<ByteIndexKey> {
    /** */
    public ByteInlineIndexKeyType() {
        super(IndexKeyType.BYTE, (short)1);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, IndexKey key) {
        byte val = PageUtils.getByte(pageAddr, off + 1);

        return -Integer.signum(((NumericIndexKey)key).compareTo(val));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, ByteIndexKey key, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte)type().code());
        PageUtils.putByte(pageAddr, off + 1, (byte)key.key());

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected ByteIndexKey get0(long pageAddr, int off) {
        byte b = PageUtils.getByte(pageAddr, off + 1);

        return new ByteIndexKey(b);
    }
}
