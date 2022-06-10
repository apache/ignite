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
import org.apache.ignite.internal.cache.query.index.sorted.keys.BytesIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.JavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.PlainJavaObjectIndexKey;

/**
 * Inline index key implementation for inlining Java Objects as byte array.
 */
public class ObjectByteArrayInlineIndexKeyType extends NullableInlineIndexKeyType<JavaObjectIndexKey> {
    /** */
    private final BytesInlineIndexKeyType delegate;

    /** */
    public ObjectByteArrayInlineIndexKeyType(BytesInlineIndexKeyType delegate) {
        super(IndexKeyType.JAVA_OBJECT, (short)-1);

        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, JavaObjectIndexKey key, int maxSize) {
        byte[] b = key.bytes();

        // Signed or unsigned doesn't matter there.
        return delegate.put0(pageAddr, off, new BytesIndexKey(b), maxSize);
    }

    /** {@inheritDoc} */
    @Override protected JavaObjectIndexKey get0(long pageAddr, int off) {
        byte[] b = (byte[])delegate.get0(pageAddr, off).key();

        return new PlainJavaObjectIndexKey(null, b);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, IndexKey key) {
        byte[] b = ((JavaObjectIndexKey)key).bytesNoCopy();

        // Signed or unsigned doesn't matter there.
        return delegate.compare0(pageAddr, off, new BytesIndexKey(b));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(JavaObjectIndexKey key) {
        byte[] b = key.bytes();

        // Signed or unsigned doesn't matter there.
        return delegate.inlineSize0(new BytesIndexKey(b));
    }
}
