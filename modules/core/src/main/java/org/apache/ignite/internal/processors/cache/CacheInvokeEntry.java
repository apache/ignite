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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MutableEntry} passed to the {@link EntryProcessor#process(MutableEntry, Object...)}.
 */
public class CacheInvokeEntry<K, V> extends CacheLazyEntry<K, V> implements MutableEntry<K, V> {
    /** */
    private final boolean hadVal;

    /** */
    private Operation op = Operation.NONE;

    /** */
    private V oldVal;

    /** Entry version. */
    private GridCacheVersion ver;

    /**
     * @param cctx Cache context.
     * @param keyObj Key cache object.
     * @param valObj Cache object value.
     * @param ver Entry version.
     */
    public CacheInvokeEntry(GridCacheContext cctx,
        KeyCacheObject keyObj,
        @Nullable CacheObject valObj,
        GridCacheVersion ver
    ) {
        super(cctx, keyObj, valObj);

        this.hadVal = valObj != null;
        this.ver = ver;
    }

    /**
     * @param ctx Cache context.
     * @param keyObj Key cache object.
     * @param key Key value.
     * @param valObj Value cache object.
     * @param val Value.
     * @param ver Entry version.
     */
    public CacheInvokeEntry(GridCacheContext<K, V> ctx,
        KeyCacheObject keyObj,
        @Nullable K key,
        @Nullable CacheObject valObj,
        @Nullable V val,
        GridCacheVersion ver) {
        super(ctx, keyObj, key, valObj, val);

        this.hadVal = valObj != null || val != null;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public boolean exists() {
        return val != null || valObj != null;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        val = null;
        valObj = null;

        if (op == Operation.CREATE)
            op = Operation.NONE;
        else
            op = Operation.REMOVE;
    }

    /** {@inheritDoc} */
    @Override public void setValue(V val) {
        if (val == null)
            throw new NullPointerException();

        this.oldVal = this.val;

        this.val = val;

        op = hadVal ? Operation.UPDATE : Operation.CREATE;
    }

    /**
     * @return Return origin value, before modification.
     */
    public V oldVal() {
        return oldVal == null ? val : oldVal;
    }

    /**
     * @return {@code True} if {@link #setValue} or {@link #remove was called}.
     */
    public boolean modified() {
        return op != Operation.NONE;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(CacheEntry.class) && ver != null)
            return (T)new CacheEntryImplEx<>(getKey(), getValue(), ver);

        return super.unwrap(cls);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeEntry.class, this);
    }

    /**
     *
     */
    private static enum Operation {
        /** */
        NONE,

        /** */
        CREATE,

        /** */
        UPDATE,

        /** */
        REMOVE
    }
}