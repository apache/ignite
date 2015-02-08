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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT cache lock response.
 */
public class GridDhtLockResponse<K, V> extends GridDistributedLockResponse<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Evicted readers. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxKey<K>> nearEvicted;

    /** Evicted reader key bytes. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearEvictedBytes;

    /** Mini ID. */
    private IgniteUuid miniId;

    /** Invalid partitions. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Set<Integer> invalidParts = new GridLeanSet<>();

    @GridDirectTransient
    /** Preload entries. */
    private List<GridCacheEntryInfo<K, V>> preloadEntries;

    /** */
    @GridDirectCollection(byte[].class)
    private List<byte[]> preloadEntriesBytes;

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
     */
    public GridDhtLockResponse(int cacheId, GridCacheVersion lockVer, IgniteUuid futId, IgniteUuid miniId, int cnt) {
        super(cacheId, lockVer, futId, cnt);

        assert miniId != null;

        this.miniId = miniId;
    }

    /**
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param err Error.
     */
    public GridDhtLockResponse(int cacheId, GridCacheVersion lockVer, IgniteUuid futId, IgniteUuid miniId, Throwable err) {
        super(cacheId, lockVer, futId, err);

        assert miniId != null;

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
     * @param nearEvictedBytes Key bytes.
     */
    public void nearEvictedBytes(Collection<byte[]> nearEvictedBytes) {
        this.nearEvictedBytes = nearEvictedBytes;
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
        invalidParts.add(part);
    }

    /**
     * @return Invalid partitions.
     */
    public Set<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * Adds preload entry to lock response.
     *
     * @param info Info to add.
     */
    public void addPreloadEntry(GridCacheEntryInfo<K, V> info) {
        if (preloadEntries == null)
            preloadEntries = new ArrayList<>();

        preloadEntries.add(info);
    }

    /**
     * Gets preload entries returned from backup.
     *
     * @return Collection of preload entries.
     */
    public Collection<GridCacheEntryInfo<K, V>> preloadEntries() {
        return preloadEntries == null ? Collections.<GridCacheEntryInfo<K, V>>emptyList() : preloadEntries;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearEvictedBytes == null && nearEvicted != null)
            nearEvictedBytes = marshalCollection(nearEvicted, ctx);

        if (preloadEntriesBytes == null && preloadEntries != null)
            preloadEntriesBytes = marshalCollection(preloadEntries, ctx);

        if (preloadEntriesBytes == null && preloadEntries != null) {
            marshalInfos(preloadEntries, ctx);

            preloadEntriesBytes = marshalCollection(preloadEntries, ctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (nearEvicted == null && nearEvictedBytes != null)
            nearEvicted = unmarshalCollection(nearEvictedBytes, ctx, ldr);

        if (preloadEntries == null && preloadEntriesBytes != null)
            preloadEntries = unmarshalCollection(preloadEntriesBytes, ctx, ldr);

        if (preloadEntries == null && preloadEntriesBytes != null) {
            preloadEntries = unmarshalCollection(preloadEntriesBytes, ctx, ldr);

            unmarshalInfos(preloadEntries, ctx.cacheContext(cacheId), ldr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridDhtLockResponse _clone = new GridDhtLockResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtLockResponse _clone = (GridDhtLockResponse)_msg;

        _clone.nearEvicted = nearEvicted;
        _clone.nearEvictedBytes = nearEvictedBytes;
        _clone.miniId = miniId;
        _clone.invalidParts = invalidParts;
        _clone.preloadEntries = preloadEntries;
        _clone.preloadEntriesBytes = preloadEntriesBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 11:
                if (!writer.writeCollection("invalidParts", invalidParts, int.class))
                    return false;

                state++;

            case 12:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                state++;

            case 13:
                if (!writer.writeCollection("nearEvictedBytes", nearEvictedBytes, byte[].class))
                    return false;

                state++;

            case 14:
                if (!writer.writeCollection("preloadEntriesBytes", preloadEntriesBytes, byte[].class))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 11:
                invalidParts = reader.readCollection("invalidParts", int.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 12:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 13:
                nearEvictedBytes = reader.readCollection("nearEvictedBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 14:
                preloadEntriesBytes = reader.readCollection("preloadEntriesBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 31;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockResponse.class, this, super.toString());
    }
}
