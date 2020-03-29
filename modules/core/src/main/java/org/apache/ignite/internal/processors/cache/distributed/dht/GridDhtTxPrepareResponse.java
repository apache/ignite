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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * DHT transaction prepare response.
 */
public class GridDhtTxPrepareResponse extends GridDistributedTxPrepareResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted readers. */
    @GridToStringInclude
    @GridDirectCollection(IgniteTxKey.class)
    private Collection<IgniteTxKey> nearEvicted;

    /** Future ID.  */
    private IgniteUuid futId;

    /** Mini future ID. */
    private int miniId;

    /** Invalid partitions by cache ID. */
    @GridDirectMap(keyType = Integer.class, valueType = int[].class)
    private Map<Integer, int[]> invalidParts;

    /** Preload entries. */
    @GridDirectCollection(GridCacheEntryInfo.class)
    private List<GridCacheEntryInfo> preloadEntries;

    /**
     * Empty constructor required by {@link Externalizable}.
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
     * @param nearEvicted Evicted readers.
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
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Map from cacheId to an array of invalid partitions.
     */
    Map<Integer, int[]> invalidPartitionsByCacheId() {
        return invalidParts;
    }

    /**
     * @param invalidPartsByCacheId Map from cache ID to an array of invalid partitions.
     */
    public void invalidPartitionsByCacheId(Map<Integer, Set<Integer>> invalidPartsByCacheId) {
        this.invalidParts = CU.convertInvalidPartitions(invalidPartsByCacheId);
    }

    /**
     * Gets preload entries found on backup node.
     *
     * @return Collection of entry infos need to be preloaded.
     */
    Collection<GridCacheEntryInfo> preloadEntries() {
        return preloadEntries == null ? Collections.<GridCacheEntryInfo>emptyList() : preloadEntries;
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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearEvicted != null) {
            for (IgniteTxKey key : nearEvicted) {
                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                // Can be null if client near cache was removed, in this case assume do not need prepareMarshal.
                if (cctx != null)
                    key.prepareMarshal(cctx);
            }
        }

        if (preloadEntries != null) {
            for (GridCacheEntryInfo info : preloadEntries) {
                GridCacheContext cctx = ctx.cacheContext(info.cacheId());

                info.marshal(cctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (nearEvicted != null) {
            for (IgniteTxKey key : nearEvicted) {
                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                key.finishUnmarshal(cctx, ldr);
            }
        }

        if (preloadEntries != null) {
            for (GridCacheEntryInfo info : preloadEntries) {
                GridCacheContext cctx = ctx.cacheContext(info.cacheId());

                info.unmarshal(cctx, ldr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 11:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap("invalidParts", invalidParts, MessageCollectionItemType.INT, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("nearEvicted", nearEvicted, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection("preloadEntries", preloadEntries, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 11:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                invalidParts = reader.readMap("invalidParts", MessageCollectionItemType.INT, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                nearEvicted = reader.readCollection("nearEvicted", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                preloadEntries = reader.readCollection("preloadEntries", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxPrepareResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 35;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareResponse.class, this,
            "super", super.toString());
    }
}
