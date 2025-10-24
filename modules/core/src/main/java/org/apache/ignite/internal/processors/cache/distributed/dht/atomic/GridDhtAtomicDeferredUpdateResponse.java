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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Deferred dht atomic update response.
 */
public class GridDhtAtomicDeferredUpdateResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** ACK future versions. */
    @Order(value = 4, method = "futureIds")
    private GridLongList futIds;

    /** */
    @GridToStringExclude
    private GridTimeoutObject timeoutSnd;

    /**
     * Empty constructor.
     */
    public GridDhtAtomicDeferredUpdateResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param futIds Future IDs.
     */
    public GridDhtAtomicDeferredUpdateResponse(int cacheId, GridLongList futIds) {
        this.cacheId = cacheId;
        this.futIds = futIds;
    }

    /**
     * @param timeoutSnd Callback sending response on timeout.
     */
    void timeoutSender(@Nullable GridTimeoutObject timeoutSnd) {
        this.timeoutSnd = timeoutSnd;
    }

    /**
     * @return Callback sending response on timeout.
     */
    @Nullable GridTimeoutObject timeoutSender() {
        return timeoutSnd;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /**
     * @return List of ACKed future ids.
     */
    public GridLongList futureIds() {
        return futIds;
    }

    /**
     * @param futIds New list of ACKed future ids.
     */
    public void futureIds(GridLongList futIds) {
        this.futIds = futIds;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 37;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicDeferredUpdateResponse.class, this);
    }
}
