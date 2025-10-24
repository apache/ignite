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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridNearSingleGetResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    public static final int INVALID_PART_FLAG_MASK = 0x1;

    /** */
    public static final int CONTAINS_VAL_FLAG_MASK = 0x2;

    /** Future ID. */
    @Order(value = 4, method = "futureId")
    private long futId;

    /** Result. */
    @Order(value = 5, method = "result")
    private Message res;

    /** Topology version. */
    @Order(value = 6, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Error message. */
    @Order(value = 7, method = "errorMessage")
    private ErrorMessage errMsg;

    /** Flags. */
    @Order(8)
    private byte flags;

    /**
     * Empty constructor.
     */
    public GridNearSingleGetResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param res Result.
     * @param invalidPartitions {@code True} if invalid partitions error occurred.
     * @param addDepInfo Deployment info.
     */
    public GridNearSingleGetResponse(
        int cacheId,
        long futId,
        AffinityTopologyVersion topVer,
        @Nullable Message res,
        boolean invalidPartitions,
        boolean addDepInfo
    ) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.topVer = topVer;
        this.res = res;
        this.addDepInfo = addDepInfo;

        if (invalidPartitions)
            flags |= INVALID_PART_FLAG_MASK;
    }

    /**
     * @param err Error.
     */
    public void error(Throwable err) {
        this.errMsg = new ErrorMessage(err);
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @return Error message.
     */
    public ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer != null ? topVer : super.topologyVersion();
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return {@code True} if invalid partitions error occurred.
     */
    public boolean invalidPartitions() {
        return (flags & INVALID_PART_FLAG_MASK) != 0;
    }

    /**
     * @return Results for request with set flag {@link GridNearSingleGetRequest#skipValues()}.
     */
    public boolean containsValue() {
        return (flags & CONTAINS_VAL_FLAG_MASK) != 0;
    }

    /**
     *
     */
    public void setContainsValue() {
        flags |= CONTAINS_VAL_FLAG_MASK;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Result.
     */
    public Message result() {
        return res;
    }

    /**
     * @param res Result.
     */
    public void result(Message res) {
        this.res = res;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (res != null) {
            GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

            if (res instanceof CacheObject)
                prepareMarshalCacheObject((CacheObject)res, cctx);
            else if (res instanceof CacheVersionedValue)
                ((CacheVersionedValue)res).prepareMarshal(cctx.cacheObjectContext());
            else if (res instanceof GridCacheEntryInfo)
                ((GridCacheEntryInfo)res).marshal(cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (res != null) {
            GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId());

            if (res instanceof CacheObject)
                ((CacheObject)res).finishUnmarshal(cctx.cacheObjectContext(), ldr);
            else if (res instanceof CacheVersionedValue)
                ((CacheVersionedValue)res).finishUnmarshal(cctx, ldr);
            else if (res instanceof GridCacheEntryInfo)
                ((GridCacheEntryInfo)res).unmarshal(cctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 117;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearSingleGetResponse.class, this);
    }
}
