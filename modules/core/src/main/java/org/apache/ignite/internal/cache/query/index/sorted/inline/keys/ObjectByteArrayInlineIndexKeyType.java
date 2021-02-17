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

/** */
public class ObjectByteArrayInlineIndexKeyType extends NullableInlineIndexKeyType<Object> {
    /**
     */
    public ObjectByteArrayInlineIndexKeyType() {
        super(IndexKeyTypes.JAVA_OBJECT, (short) -1);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Object val, int maxSize) {
        return 0;
        //        return bytesType.put0(pageAddr, off, fromObject(val), maxSize);
    }

    /** {@inheritDoc} */
    @Override protected Object get0(long pageAddr, int off) {
//        byte[] arr = bytesType.get0(pageAddr, off);
        return null;

//        return ValueJavaObject.getNoCopy(null, readBytes(pageAddr, off), null);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Object v) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Object key) {
        return 0;
//        return bytesType.inlineSize0(fromObject(key));
    }

    /** */
    private byte[] fromObject(Object o) {
        return new byte[0];
    }
}
