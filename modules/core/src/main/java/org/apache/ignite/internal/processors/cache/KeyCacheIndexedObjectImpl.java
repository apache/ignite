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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;

/**
 * Cache object implementation for classes that support footer injection is their serialized form thus enabling fields
 * search and extraction without necessity to fully deserialize an object.
 */
public class KeyCacheIndexedObjectImpl extends CacheIndexedObjectImpl implements KeyCacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public KeyCacheIndexedObjectImpl() {
        // No-op
    }

    /**
     * @param val Object.
     * @param valBytes Object in a serialized form.
     */
    public KeyCacheIndexedObjectImpl(CacheObjectContext objCtx, Object val, byte[] valBytes) {
        super(objCtx, val, valBytes);

        assert val != null;
    }

    /**
     * @param val Object.
     * @param valBytes Object in a serialized form.
     * @param start Object's start in the array.
     * @param len Object's len in the array.
     */
    public KeyCacheIndexedObjectImpl(CacheObjectContext objCtx, Object val, byte[] valBytes, int start, int len) {
        super(objCtx, val, valBytes, start, len);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (val == null)
            val = ctx.processor().unmarshal(ctx, valBytes, start, len, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        // refer to GridIoMessageFactory.
        return 113;
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        assert val != null;

        return val instanceof GridCacheInternal;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert val != null;

        return val.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof KeyCacheIndexedObjectImpl))
            return false;

        KeyCacheIndexedObjectImpl other = (KeyCacheIndexedObjectImpl)obj;

        return val.equals(other.val);
    }

    /** {@inheritDoc} */
    @Override protected boolean keepDeserialized(CacheObjectContext ctx, boolean checkCls) {
        return true;
    }
}
