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
import org.apache.ignite.internal.cache.query.index.sorted.keys.StringIndexKey;
import org.jetbrains.annotations.Nullable;

/**
 * Skip optimized String comparison implemented in {@link StringInlineIndexKeyType}.
 */
public class StringNoCompareInlineIndexKeyType extends NullableInlineIndexKeyType<StringIndexKey> {
    /** Delegate all String operations except comparison to StringInlineIndexKeyType. */
    private final StringInlineIndexKeyType delegate = new StringInlineIndexKeyType();

    /** */
    public StringNoCompareInlineIndexKeyType() {
        super(IndexKeyType.STRING, (short)-1);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, StringIndexKey key, int maxSize) {
        return delegate.put0(pageAddr, off, key, maxSize);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable StringIndexKey get0(long pageAddr, int off) {
        return delegate.get0(pageAddr, off);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, IndexKey key) {
        return COMPARE_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(StringIndexKey key) {
        return delegate.inlineSize0(key);
    }

    /** {@inheritDoc} */
    @Override public boolean inlinedFullValue(long pageAddr, int off) {
        return delegate.inlinedFullValue(pageAddr, off);
    }
}
