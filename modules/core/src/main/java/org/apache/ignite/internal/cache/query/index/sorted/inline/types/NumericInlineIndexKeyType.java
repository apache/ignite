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
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NumericIndexKey;

/**
 * Inline index key implementation for inlining numeric values.
 */
public abstract class NumericInlineIndexKeyType<T extends IndexKey> extends NullableInlineIndexKeyType<T> {
    /** Constructor. */
    protected NumericInlineIndexKeyType(IndexKeyType type, short keySize) {
        super(type, keySize);
    }

    /** {@inheritDoc} */
    @Override public boolean isComparableTo(IndexKey key) {
        return key instanceof NumericIndexKey;
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, IndexKey key) {
        return -Integer.signum(compareNumeric((NumericIndexKey)key, pageAddr, off));
    }

    /** Compare numeric index key with inlined value. */
    public abstract int compareNumeric(NumericIndexKey key, long pageAddr, int off);

    /** {@inheritDoc} */
    @Override protected int inlineSize0(T key) {
        return keySize + 1;
    }
}
