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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT transaction prepare response.
 */
public class GridDhtTxPrepareResponse<K, V> extends GridDistributedTxPrepareResponse<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted readers. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxKey<K>> nearEvicted;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearEvictedBytes;

    /** Future ID.  */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Invalid partitions. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts;

    @GridDirectTransient
    /** Preload entries. */
    private List<GridCacheEntryInfo<K, V>> preloadEntries;

    /** */
    @GridDirectCollection(byte[].class)
    private List<byte[]> preloadEntriesBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtTxPrepareResponse() {
        // No-op.
    }

    /**
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridDhtTxPrepareResponse(GridCacheVersion xid, IgniteUuid futId, IgniteUuid miniId) {
        super(xid);

        assert futId != null;
        assert miniId != null;

        this.futId = futId;
        this.miniId = miniId;
    }

    /**
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param err Error.
     */
    public GridDhtTxPrepareResponse(GridCacheVersion xid, IgniteUuid futId, IgniteUuid miniId, Throwable err) {
        super(xid, err);

        assert futId != null;
        assert miniId != null;

        this.futId = futId;
        this.miniId = miniId;
    }

    /**
     * @return Evicted readers.
     */
    public Collection<IgniteTxKey<K>> nearEvicted() {
        return nearEvicted;
    }

    /**
     * @param nearEvicted Evicted readers.
     */
    public void nearEvicted(Collection<IgniteTxKey<K>> nearEvicted) {
        this.nearEvicted = nearEvicted;
    }

    /**
     * @param nearEvictedBytes Near evicted bytes.
     */
    public void nearEvictedBytes(Collection<byte[]> nearEvictedBytes) {
        this.nearEvictedBytes = nearEvictedBytes;
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
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Invalid partitions.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param invalidParts Invalid partitions.
     */
    public void invalidPartitions(Collection<Integer> invalidParts) {
        this.invalidParts = invalidParts;
    }

    /**
     * Gets preload entries found on backup node.
     *
     * @return Collection of entry infos need to be preloaded.
     */
    public Collection<GridCacheEntryInfo<K, V>> preloadEntries() {
        return preloadEntries == null ? Collections.<GridCacheEntryInfo<K, V>>emptyList() : preloadEntries;
    }

    /**
     * Adds preload entry.
     *
     * @param info Info to add.
     */
    public void addPreloadEntry(GridCacheEntryInfo<K, V> info) {
        assert info.cacheId() != 0;

        if (preloadEntries == null)
            preloadEntries = new ArrayList<>();

        preloadEntries.add(info);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearEvictedBytes == null)
            nearEvictedBytes = marshalCollection(nearEvicted, ctx);

        if (preloadEntriesBytes == null && preloadEntries != null)
            preloadEntriesBytes = marshalCollection(preloadEntries, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        // Unmarshal even if deployment is disabled, since we could get bytes initially.
        if (nearEvicted == null && nearEvictedBytes != null)
            nearEvicted = unmarshalCollection(nearEvictedBytes, ctx, ldr);

        if (preloadEntries == null && preloadEntriesBytes != null)
            preloadEntries = unmarshalCollection(preloadEntriesBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareResponse.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(new MessageHeader(directType(), (byte)15)))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 10:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection("invalidParts", invalidParts, Type.INT))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection("nearEvictedBytes", nearEvictedBytes, Type.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("preloadEntriesBytes", preloadEntriesBytes, Type.BYTE_ARR))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 10:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                invalidParts = reader.readCollection("invalidParts", Type.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                nearEvictedBytes = reader.readCollection("nearEvictedBytes", Type.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                preloadEntriesBytes = reader.readCollection("preloadEntriesBytes", Type.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 35;
    }
}
