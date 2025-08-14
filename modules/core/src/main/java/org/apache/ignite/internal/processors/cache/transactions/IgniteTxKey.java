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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Cache transaction key. This wrapper is needed because same keys may be enlisted in the same transaction
 * for multiple caches.
 */
public class IgniteTxKey implements Message {
    /** Key. */
    @Order(0)
    @GridToStringInclude(sensitive = true)
    private KeyCacheObject key;

    /** Cache ID. */
    @Order(1)
    private int cacheId;

    /**
     * Empty constructor.
     */
    public IgniteTxKey() {
        // No-op.
    }

    /**
     * @param key User key.
     * @param cacheId Cache ID.
     */
    public IgniteTxKey(KeyCacheObject key, int cacheId) {
        this.key = key;
        this.cacheId = cacheId;
    }

    /**
     * @return User key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @param key New key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param cacheId New cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        key.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert key != null;

        key.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteTxKey))
            return false;

        IgniteTxKey that = (IgniteTxKey)o;

        return cacheId == that.cacheId && key.equals(that.key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = key.hashCode();

        res = 31 * res + cacheId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 94;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxKey.class, this);
    }
}
