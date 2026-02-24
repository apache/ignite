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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Near cache lock response.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridNearLockResponse extends GridDistributedLockResponse {
    /** Pending versions that are less than {@link #version()}. */
    @GridToStringInclude
    @Order(10)
    Collection<GridCacheVersion> pending;

    /** Mini future ID. */
    @Order(11)
    int miniId;

    /** DHT versions. */
    @GridToStringInclude
    @Order(12)
    GridCacheVersion[] dhtVers;

    /** DHT candidate versions. */
    @GridToStringInclude
    @Order(13)
    GridCacheVersion[] mappedVers;

    /** Filter evaluation results for fast-commit transactions. */
    @Order(14)
    boolean[] filterRes;

    /** Topology version, which is set when client node should remap lock request. */
    @Order(15)
    AffinityTopologyVersion clientRemapVer;

    /**
     * Flag, indicating whether remap version is compatible with current version.
     * Used together with clientRemapVer.
     */
    @Order(16)
    boolean compatibleRemapVer;

    /**
     * Empty constructor.
     */
    public GridNearLockResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param filterRes {@code True} if need to allocate array for filter evaluation results.
     * @param cnt Count.
     * @param err Error.
     * @param clientRemapVer {@code True} if client node should remap lock request. If {@code compatibleRemapVer} is
     * {@code true} when first request is not remapped, but all subsequent will use remap version.
     * @param addDepInfo Deployment info.
     * @param compatibleRemapVer {@code True} if remap version is compatible with lock version.
     */
    public GridNearLockResponse(
        int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        int miniId,
        boolean filterRes,
        int cnt,
        Throwable err,
        AffinityTopologyVersion clientRemapVer,
        boolean addDepInfo,
        boolean compatibleRemapVer
    ) {
        super(cacheId, lockVer, futId, cnt, err, addDepInfo);

        assert miniId != 0;

        this.miniId = miniId;
        this.clientRemapVer = clientRemapVer;

        dhtVers = new GridCacheVersion[cnt];
        mappedVers = new GridCacheVersion[cnt];

        if (filterRes)
            this.filterRes = new boolean[cnt];

        this.compatibleRemapVer = compatibleRemapVer;
    }

    /**
     * @return Topology version, which is set when client node should remap lock request.
     */
    @Nullable public AffinityTopologyVersion clientRemapVersion() {
        return clientRemapVer;
    }

    /**
     * @param clientRemapVer New topology version, which is set when client node should remap lock request.
     */
    public void clientRemapVersion(AffinityTopologyVersion clientRemapVer) {
        this.clientRemapVer = clientRemapVer;
    }

    /**
     * @return Flag, indicating whether remap version is compatible with current version.
     */
    public boolean compatibleRemapVersion() {
        return compatibleRemapVer;
    }

    /**
     * @param compatibleRemapVer New flag, indicating whether remap version is compatible with current version.
     */
    public void compatibleRemapVersion(boolean compatibleRemapVer) {
        this.compatibleRemapVer = compatibleRemapVer;
    }

    /**
     * @return Pending versions that are less than {@link #version()}.
     */
    public Collection<GridCacheVersion> pending() {
        return pending;
    }

    /**
     * @param pending New pending versions that are less than {@link #version()}.
     */
    public void pending(Collection<GridCacheVersion> pending) {
        this.pending = pending;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId New mini future ID.
     */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @param idx Index.
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion(int idx) {
        return dhtVers == null ? null : dhtVers[idx];
    }

    /**
     * @return DHT versions.
     */
    public GridCacheVersion[] dhtVersions() {
        return dhtVers;
    }

    /**
     * @param dhtVers New DHT versions.
     */
    public void dhtVersions(GridCacheVersion[] dhtVers) {
        this.dhtVers = dhtVers;
    }

    /**
     * Returns DHT candidate version for acquired near lock on DHT node.
     *
     * @param idx Key index.
     * @return DHT version.
     */
    public GridCacheVersion mappedVersion(int idx) {
        return mappedVers == null ? null : mappedVers[idx];
    }

    /**
     * @return DHT candidate versions.
     */
    public GridCacheVersion[] mappedVersions() {
        return mappedVers;
    }

    /**
     * @param mappedVers New DHT candidate versions.
     */
    public void mappedVersions(GridCacheVersion[] mappedVers) {
        this.mappedVers = mappedVers;
    }

    /**
     * Gets filter evaluation result for fast-commit transaction.
     *
     * @param idx Result index.
     * @return {@code True} if filter passed on primary node, {@code false} otherwise.
     */
    public boolean filterResult(int idx) {
        assert filterRes != null : "Should not call filterResult for non-fast-commit transactions.";

        return filterRes[idx];
    }

    /**
     * @return Filter evaluation results for fast-commit transactions.
     */
    public boolean[] filterResults() {
        return filterRes;
    }

    /**
     * @param filterRes New filter evaluation results for fast-commit transactions.
     */
    public void filterResults(boolean[] filterRes) {
        this.filterRes = filterRes;
    }

    /**
     * @param val Value.
     * @param filterPassed Boolean flag indicating whether filter passed for fast-commit transaction.
     * @param dhtVer DHT version.
     * @param mappedVer Mapped version.
     * @throws IgniteCheckedException If failed.
     */
    public void addValueBytes(
        @Nullable CacheObject val,
        boolean filterPassed,
        @Nullable GridCacheVersion dhtVer,
        @Nullable GridCacheVersion mappedVer
    ) throws IgniteCheckedException {
        int idx = valuesSize();

        dhtVers[idx] = dhtVer;
        mappedVers[idx] = mappedVer;

        if (filterRes != null)
            filterRes[idx] = filterPassed;

        // Delegate to super.
        addValue(val);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 52;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockResponse.class, this, super.toString());
    }
}
