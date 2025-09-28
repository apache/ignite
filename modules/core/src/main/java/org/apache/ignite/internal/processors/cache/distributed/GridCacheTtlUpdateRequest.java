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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class GridCacheTtlUpdateRequest extends GridCacheIdMessage {
    /** Entries keys. */
    @GridToStringInclude
    @Order(4)
    private List<KeyCacheObject> keys;

    /** Entries versions. */
    @Order(value = 5, method = "versions")
    private List<GridCacheVersion> vers;

    /** Near entries keys. */
    @GridToStringInclude
    @Order(6)
    private List<KeyCacheObject> nearKeys;

    /** Near entries versions. */
    @Order(value = 7, method = "nearVersions")
    private List<GridCacheVersion> nearVers;

    /** New TTL. */
    @Order(8)
    private long ttl;

    /** Topology version. */
    @Order(value = 9, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /**
     * Required empty constructor.
     */
    public GridCacheTtlUpdateRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @param ttl TTL.
     */
    public GridCacheTtlUpdateRequest(int cacheId, AffinityTopologyVersion topVer, long ttl) {
        assert ttl >= 0 || ttl == CU.TTL_ZERO : ttl;

        this.cacheId = cacheId;
        this.topVer = topVer;
        this.ttl = ttl;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return TTL.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param ttl New new TTL.
     */
    public void ttl(long ttl) {
        this.ttl = ttl;
    }

    /**
     * @param key Key.
     * @param ver Version.
     */
    public void addEntry(KeyCacheObject key, GridCacheVersion ver) {
        if (keys == null) {
            keys = new ArrayList<>();

            vers = new ArrayList<>();
        }

        keys.add(key);

        vers.add(ver);
    }

    /**
     * @param key Key.
     * @param ver Version.
     */
    public void addNearEntry(KeyCacheObject key, GridCacheVersion ver) {
        if (nearKeys == null) {
            nearKeys = new ArrayList<>();

            nearVers = new ArrayList<>();
        }

        nearKeys.add(key);

        nearVers.add(ver);
    }

    /**
     * @return Keys.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param keys New entries keys.
     */
    public void keys(List<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /**
     * @return Versions.
     */
    public List<GridCacheVersion> versions() {
        return vers;
    }

    /**
     * @param idx Entry index.
     * @return Version.
     */
    public GridCacheVersion version(int idx) {
        assert idx >= 0 && idx < vers.size() : idx;

        return vers.get(idx);
    }

    /**
     * @param vers New entries versions.
     */
    public void versions(List<GridCacheVersion> vers) {
        this.vers = vers;
    }

    /**
     * @return Keys for near cache.
     */
    public List<KeyCacheObject> nearKeys() {
        return nearKeys;
    }

    /**
     * @param nearKeys New near entries keys.
     */
    public void nearKeys(List<KeyCacheObject> nearKeys) {
        this.nearKeys = nearKeys;
    }

    /**
     * @return Versions for near cache entries.
     */
    public List<GridCacheVersion> nearVersions() {
        return nearVers;
    }

    /**
     * @param nearVers New near entries versions.
     */
    public void nearVersions(List<GridCacheVersion> nearVers) {
        this.nearVers = nearVers;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);

        prepareMarshalCacheObjects(nearKeys, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        finishUnmarshalCacheObjects(nearKeys, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTtlUpdateRequest.class, this, "super", super.toString());
    }
}
