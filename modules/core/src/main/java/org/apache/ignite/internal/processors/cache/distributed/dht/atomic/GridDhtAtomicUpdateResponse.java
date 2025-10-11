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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DHT atomic cache backup update response.
 */
public class GridDhtAtomicUpdateResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Future version. */
    @Order(value = 4, method = "futureId")
    private long futId;

    /** */
    @Order(value = 5, method = "errors")
    private UpdateErrors errs;

    /** Evicted readers. */
    @GridToStringInclude
    @Order(6)
    private List<KeyCacheObject> nearEvicted;

    /** */
    private int partId;

    /**
     * Empty constructor.
     */
    public GridDhtAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition.
     * @param futId Future ID.
     * @param addDepInfo Deployment info.
     */
    public GridDhtAtomicUpdateResponse(int cacheId, int partId, long futId, boolean addDepInfo) {
        this.cacheId = cacheId;
        this.partId = partId;
        this.futId = futId;
        this.addDepInfo = addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Future version.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @param futId New future version.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /**
     * @return Errors.
     */
    public UpdateErrors errors() {
        return errs;
    }

    /**
     * @param errs Errs.
     */
    public void errors(UpdateErrors errs) {
        this.errs = errs;
    }

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    public void onError(IgniteCheckedException err) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.onError(err);
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return errs != null ? errs.error() : null;
    }

    /**
     * @return Evicted readers.
     */
    public Collection<KeyCacheObject> nearEvicted() {
        return nearEvicted;
    }

    /**
     * @param nearEvicted Evicted near cache keys.
     */
    public void nearEvicted(List<KeyCacheObject> nearEvicted) {
        this.nearEvicted = nearEvicted;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        // Can be null if client near cache was removed, in this case assume do not need prepareMarshal.
        if (cctx != null) {
            prepareMarshalCacheObjects(nearEvicted, cctx);

            if (errs != null)
                errs.prepareMarshal(this, cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(nearEvicted, cctx, ldr);

        if (errs != null)
            errs.finishUnmarshal(this, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 39;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateResponse.class, this);
    }
}
