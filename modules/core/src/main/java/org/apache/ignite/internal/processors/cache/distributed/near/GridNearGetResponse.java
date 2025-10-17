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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Get response.
 */
public final class GridNearGetResponse extends GridCacheIdMessage implements GridCacheDeployable, GridCacheVersionable {
    /** Future ID. */
    @Order(value = 4, method = "futureId")
    private IgniteUuid futId;

    /** Sub ID. */
    @Order(5)
    private IgniteUuid miniId;

    /** Version. */
    @Order(value = 6, method = "version")
    private GridCacheVersion ver;

    /** Result. */
    @GridToStringInclude
    @Order(7)
    private Collection<GridCacheEntryInfo> entries;

    /** Keys to retry due to ownership shift. */
    @GridToStringInclude
    @Order(value = 8, method = "invalidPartitions")
    private Collection<Integer> invalidParts = new GridLeanSet<>();

    /** Topology version if invalid partitions is not empty. */
    @Order(value = 9, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Error message. */
    @Order(value = 10, method = "errorMessage")
    private @Nullable ErrorMessage errMsg;

    /**
     * Empty constructor.
     */
    public GridNearGetResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     * @param addDepInfo Deployment info.
     */
    public GridNearGetResponse(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        boolean addDepInfo
    ) {
        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;
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
     * @return Sub ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Sub ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @return Entries.
     */
    public Collection<GridCacheEntryInfo> entries() {
        return entries != null ? entries : Collections.emptyList();
    }

    /**
     * @param entries Entries.
     */
    public void entries(Collection<GridCacheEntryInfo> entries) {
        this.entries = entries;
    }

    /**
     * @return Failed filter set.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param invalidParts Failed filter set.
     */
    public void invalidPartitions(Collection<Integer> invalidParts) {
        this.invalidParts = invalidParts;
    }

    /**
     * @param invalidParts Partitions to retry due to ownership shift.
     * @param topVer Topology version.
     */
    public void invalidPartitions(Collection<Integer> invalidParts, @NotNull AffinityTopologyVersion topVer) {
        this.invalidParts = invalidParts;
        this.topVer = topVer;
    }

    /**
     * @return Topology version if this response has invalid partitions.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer != null ? topVer : super.topologyVersion();
    }

    /**
     * @param topVer Topology version if this response has invalid partitions.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @param err Error.
     */
    public void error(@Nullable Throwable err) {
        errMsg = new ErrorMessage(err);
    }

    /**
     * @return Error message.
     */
    public @Nullable ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(@Nullable ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (entries != null) {
            for (GridCacheEntryInfo info : entries)
                info.marshal(cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId());

        if (entries != null) {
            for (GridCacheEntryInfo info : entries)
                info.unmarshal(cctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 50;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetResponse.class, this);
    }
}
