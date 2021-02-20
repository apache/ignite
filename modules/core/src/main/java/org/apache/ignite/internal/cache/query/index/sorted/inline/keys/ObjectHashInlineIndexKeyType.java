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

import org.apache.ignite.internal.cache.query.index.sorted.JavaObjectKey;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining hash of Java objects.
 */
public class ObjectHashInlineIndexKeyType extends NullableInlineIndexKeyType<Object> {
    /** */
    public ObjectHashInlineIndexKeyType() {
        super(IndexKeyTypes.JAVA_OBJECT, (short) 4);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Object val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putInt(pageAddr, off + 1, val.hashCode());

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected Object get0(long pageAddr, int off) {
        // Returns hash code of object.
        int hash = PageUtils.getInt(pageAddr, off + 1);

        return new JavaObjectKey(hash);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Object v) {
        int val1 = PageUtils.getInt(pageAddr, off + 1);
        int val2 = v.hashCode();

        int res = Integer.signum(Integer.compare(val1, val2));

        return res == 0 ? CANT_BE_COMPARE : res;
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Object key) {
        return keySize + 1;
    }
}
