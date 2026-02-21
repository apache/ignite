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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * DHT cache lock response.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridDhtLockResponse extends GridDistributedLockResponse {
    /** Mini future ID. */
    @Order(10)
    IgniteUuid miniId;

    /** Invalid partitions. */
    @GridToStringInclude
    @Order(11)
    Collection<Integer> invalidParts;

    /** Preload entries returned from backup. */
    @Order(12)
    List<GridCacheEntryInfo> preloadEntries;

    /**
     * Empty constructor.
     */
    public GridDhtLockResponse() {
        // No-op.
    }

    /**
     * @param lockVer Lock version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param cnt Key count.
     * @param addDepInfo Deployment info.
     */
    public GridDhtLockResponse(int cacheId, GridCacheVersion lockVer, IgniteUuid futId, IgniteUuid miniId, int cnt,
        boolean addDepInfo) {
        super(cacheId, lockVer, futId, cnt, addDepInfo);

        assert miniId != null;

        this.miniId = miniId;
    }

    /**
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param err Error.
     * @param addDepInfo Deployment info.
     */
    public GridDhtLockResponse(int cacheId, GridCacheVersion lockVer, IgniteUuid futId, IgniteUuid miniId,
        Throwable err, boolean addDepInfo) {
        super(cacheId, lockVer, futId, err, addDepInfo);

        assert miniId != null;

        this.miniId = miniId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId New mini future ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @param part Invalid partition.
     */
    public void addInvalidPartition(int part) {
        if (invalidParts == null)
            invalidParts = new HashSet<>();

        invalidParts.add(part);
    }

    /**
     * @return Invalid partitions.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param invalidParts New invalid partitions.
     */
    public void invalidPartitions(Collection<Integer> invalidParts) {
        this.invalidParts = invalidParts;
    }

    /**
     * Adds preload entry to lock response.
     *
     * @param info Info to add.
     */
    public void addPreloadEntry(GridCacheEntryInfo info) {
        if (preloadEntries == null)
            preloadEntries = new ArrayList<>();

        preloadEntries.add(info);
    }

    /**
     * @return Preload entries returned from backup.
     */
    public Collection<GridCacheEntryInfo> preloadEntries() {
        return preloadEntries;
    }

    /**
     * @param preloadEntries New preload entries returned from backup.
     */
    public void preloadEntries(List<GridCacheEntryInfo> preloadEntries) {
        this.preloadEntries = preloadEntries;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (preloadEntries != null)
            marshalInfos(preloadEntries, cctx.shared(), cctx.cacheObjectContext());
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (preloadEntries != null)
            unmarshalInfos(preloadEntries, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 31;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockResponse.class, this, super.toString());
    }
}
