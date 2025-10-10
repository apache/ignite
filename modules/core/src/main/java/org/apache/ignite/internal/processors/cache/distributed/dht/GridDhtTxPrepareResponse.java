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
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * DHT transaction prepare response.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridDhtTxPrepareResponse extends GridDistributedTxPrepareResponse {
    /** Evicted readers. */
    @GridToStringInclude
    @Order(9)
    private Collection<IgniteTxKey> nearEvicted;

    /** Future ID.  */
    @Order(value = 10, method = "futureId")
    private IgniteUuid futId;

    /** Mini future ID. */
    @Order(11)
    private int miniId;

    /** Invalid partitions by cache ID. */
    @Order(value = 12, method = "invalidPartitions")
    private Map<Integer, int[]> invalidParts;

    /** Preload entries found on backup node. */
    @Order(13)
    private List<GridCacheEntryInfo> preloadEntries;

    /**
     * Empty constructor.
     */
    public GridDhtTxPrepareResponse() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param addDepInfo Deployment info flag.
     */
    public GridDhtTxPrepareResponse(
        int part,
        GridCacheVersion xid,
        IgniteUuid futId,
        int miniId,
        boolean addDepInfo) {
        super(part, xid, addDepInfo);

        assert futId != null;
        assert miniId != 0;

        this.futId = futId;
        this.miniId = miniId;
    }

    /**
     * @param part Partition.
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param err Error.
     * @param addDepInfo Deployment enabled.
     */
    public GridDhtTxPrepareResponse(
        int part,
        GridCacheVersion xid,
        IgniteUuid futId,
        int miniId,
        Throwable err,
        boolean addDepInfo) {
        super(part, xid, err, addDepInfo);

        assert futId != null;
        assert miniId != 0;

        this.futId = futId;
        this.miniId = miniId;
    }

    /**
     * @return Evicted readers.
     */
    public Collection<IgniteTxKey> nearEvicted() {
        return nearEvicted;
    }

    /**
     * @param nearEvicted New evicted readers.
     */
    public void nearEvicted(Collection<IgniteTxKey> nearEvicted) {
        this.nearEvicted = nearEvicted;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId New future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
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
     * @return Invalid partitions by cache ID.
     */
    public Map<Integer, int[]> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param invalidParts New invalid partitions by cache ID.
     */
    public void invalidPartitions(Map<Integer, int[]> invalidParts) {
        this.invalidParts = invalidParts;
    }

    /**
     * @return Preload entries found on backup node.
     */
    public Collection<GridCacheEntryInfo> preloadEntries() {
        return preloadEntries;
    }

    /**
     * @param preloadEntries New preload entries found on backup node.
     */
    public void preloadEntries(List<GridCacheEntryInfo> preloadEntries) {
        this.preloadEntries = preloadEntries;
    }

    /**
     * Adds preload entry.
     *
     * @param info Info to add.
     */
    public void addPreloadEntry(GridCacheEntryInfo info) {
        assert info.cacheId() != 0;

        if (preloadEntries == null)
            preloadEntries = new ArrayList<>();

        preloadEntries.add(info);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearEvicted != null) {
            for (IgniteTxKey key : nearEvicted) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                // Can be null if client near cache was removed, in this case assume do not need prepareMarshal.
                if (cctx != null)
                    key.prepareMarshal(cctx);
            }
        }

        if (preloadEntries != null) {
            for (GridCacheEntryInfo info : preloadEntries) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(info.cacheId());

                info.marshal(cctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (nearEvicted != null) {
            for (IgniteTxKey key : nearEvicted) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                key.finishUnmarshal(cctx, ldr);
            }
        }

        if (preloadEntries != null) {
            for (GridCacheEntryInfo info : preloadEntries) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(info.cacheId());

                info.unmarshal(cctx, ldr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 35;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareResponse.class, this,
            "super", super.toString());
    }
}
