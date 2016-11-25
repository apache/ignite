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
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * DHT cache lock response.
 */
public class GridDhtLockResponse extends GridDistributedLockResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted readers. */
    @GridToStringInclude
    @GridDirectCollection(IgniteTxKey.class)
    private Collection<IgniteTxKey> nearEvicted;

    /** Mini ID. */
    private IgniteUuid miniId;

    /** Invalid partitions. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts;

    /** Preload entries. */
    @GridDirectCollection(GridCacheEntryInfo.class)
    private List<GridCacheEntryInfo> preloadEntries;

    /**
     * Empty constructor (required by {@link Externalizable}).
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
     * @param addDepInfo
     */
    public GridDhtLockResponse(int cacheId, GridCacheVersion lockVer, IgniteUuid futId, IgniteUuid miniId,
        Throwable err, boolean addDepInfo) {
        super(cacheId, lockVer, futId, err, addDepInfo);

        assert miniId != null;

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
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
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
        return invalidParts == null ? Collections.<Integer>emptySet() : invalidParts;
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
     * Gets preload entries returned from backup.
     *
     * @return Collection of preload entries.
     */
    public Collection<GridCacheEntryInfo> preloadEntries() {
        return preloadEntries == null ? Collections.<GridCacheEntryInfo>emptyList() : preloadEntries;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (nearEvicted != null) {
            for (IgniteTxKey key : nearEvicted)
                key.prepareMarshal(cctx);
        }

        if (preloadEntries != null)
            marshalInfos(preloadEntries, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (nearEvicted != null) {
            for (IgniteTxKey key : nearEvicted)
                key.finishUnmarshal(cctx, ldr);
        }

        if (preloadEntries != null)
            unmarshalInfos(preloadEntries, ctx.cacheContext(cacheId), ldr);
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
            case 10:
                if (!writer.writeCollection("invalidParts", invalidParts, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection("nearEvicted", nearEvicted, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 13:
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
            case 10:
                invalidParts = reader.readCollection("invalidParts", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                nearEvicted = reader.readCollection("nearEvicted", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                preloadEntries = reader.readCollection("preloadEntries", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtLockResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 31;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 14;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockResponse.class, this, super.toString());
    }
}
