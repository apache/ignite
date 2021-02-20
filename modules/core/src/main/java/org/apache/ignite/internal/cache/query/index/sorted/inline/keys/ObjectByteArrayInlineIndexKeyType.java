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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.inline.JavaObjectKeySerializer;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;

/**
 * Inline index key implementation for inlining Java Objects as byte array.
 */
public class ObjectByteArrayInlineIndexKeyType extends NullableInlineIndexKeyType<Object> {
    /** */
    private final BytesInlineIndexKeyType delegate = new BytesInlineIndexKeyType();

    /** */
    private final JavaObjectKeySerializer serializer = GridIndexingManager.serializer;

    /** */
    public ObjectByteArrayInlineIndexKeyType() {
        super(IndexKeyTypes.JAVA_OBJECT, (short) -1);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Object val, int maxSize) {
        try {
            byte[] b = serializer.serialize(val);

            return delegate.put0(pageAddr, off, b, maxSize);

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to serialize Java Object to byte array", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object get0(long pageAddr, int off) {
        try {
            byte[] b = delegate.get0(pageAddr, off);

            return serializer.deserialize(b);

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to deserialize Java Object from byte array", e);
        }
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Object v) {
        try {
            byte[] b = serializer.serialize(v);

            return delegate.compare0(pageAddr, off, b);

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to serialize Java Object to byte array", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Object key) {
        try {
            byte[] b = serializer.serialize(key);

            return delegate.inlineSize0(b);

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to serialize Java Object to byte array", e);
        }
    }
}
