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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Force keys request. This message is sent by node while preloading to force another node to put given keys into the
 * next batch of transmitting entries.
 */
public class GridDhtForceKeysRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** Future ID. */
    @Order(value = 4, method = "futureId")
    private IgniteUuid futId;

    /** Mini-future ID. */
    @Order(5)
    private IgniteUuid miniId;

    /** Keys to request. */
    @Order(6)
    @GridToStringInclude
    private Collection<KeyCacheObject> keys;

    /** Topology version for which keys are requested. */
    @Order(value = 7, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /**
     * Empty constructor.
     */
    public GridDhtForceKeysRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Mini-future ID.
     * @param keys Keys.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info.
     */
    GridDhtForceKeysRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        assert futId != null;
        assert miniId != null;
        assert !F.isEmpty(keys);
        assert false;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.keys = keys;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Mini-future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini-future ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Keys.
     */
    public Collection<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param keys Keys.
     */
    public void keys(Collection<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /**
     * @return Topology version for which keys are requested.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version for which keys are requested.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Key count.
     */
    private int keyCount() {
        return keys.size();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 42;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtForceKeysRequest.class, this, "keyCnt", keyCount(), "super", super.toString());
    }
}
