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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Lock request message.
 */
public abstract class GridDistributedUnlockRequest extends GridDistributedBaseMessage {
    /** Keys. */
    @GridToStringInclude
    @Order(7)
    private List<KeyCacheObject> keys;

    /**
     * Empty constructor.
     */
    public GridDistributedUnlockRequest() {
        /* No-op. */
    }

    /**
     * @param cacheId Cache ID.
     * @param keyCnt Key count.
     */
    public GridDistributedUnlockRequest(int cacheId, int keyCnt) {
        super(keyCnt, false);

        this.cacheId = cacheId;
    }

    /**
     * Sets the keys.
     */
    public void keys(List<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /**
     * @return Keys.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param key Key.
     */
    public void addKey(KeyCacheObject key) {
        if (keys == null)
            keys = new ArrayList<>(keysCount());

        keys.add(key);
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return keys != null && !keys.isEmpty() ? keys.get(0).partition() : -1;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        prepareMarshalCacheObjects(keys, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        finishUnmarshalCacheObjects(keys, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txLockMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 27;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedUnlockRequest.class, this, "super", super.toString());
    }
}
