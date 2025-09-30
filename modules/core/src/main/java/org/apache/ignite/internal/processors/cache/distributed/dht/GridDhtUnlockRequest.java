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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequest;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DHT cache unlock request.
 */
public class GridDhtUnlockRequest extends GridNearUnlockRequest {
    /** Near keys. */
    @Order(8)
    private List<KeyCacheObject> nearKeys;

    /**
     * Empty constructor.
     */
    public GridDhtUnlockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param dhtCnt Key count.
     */
    public GridDhtUnlockRequest(int cacheId, int dhtCnt) {
        super(cacheId, dhtCnt);
    }

    /**
     * Sets the near keys.
     */
    public void nearKeys(List<KeyCacheObject> nearKeys) {
        this.nearKeys = nearKeys;
    }

    /**
     * @return Near keys.
     */
    public List<KeyCacheObject> nearKeys() {
        return nearKeys;
    }

    /**
     * Adds a Near key.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void addNearKey(KeyCacheObject key)
        throws IgniteCheckedException {
        if (nearKeys == null)
            nearKeys = new ArrayList<>();

        nearKeys.add(key);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        prepareMarshalCacheObjects(nearKeys, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        finishUnmarshalCacheObjects(nearKeys, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtUnlockRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 36;
    }
}
