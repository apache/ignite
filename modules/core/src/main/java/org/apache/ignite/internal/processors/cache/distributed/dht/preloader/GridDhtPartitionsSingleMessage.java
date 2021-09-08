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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of a single node. <br>
 *
 * Sent in response to {@link GridDhtPartitionsSingleRequest} and during processing partitions exchange future.
 */
public class GridDhtPartitionsSingleMessage extends GridDhtPartitionsAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local partitions. Serialized as {@link #partsBytes}, may be compressed. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, GridDhtPartitionMap> parts;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = Integer.class)
    private Map<Integer, Integer> dupPartsData;

    /** Serialized local partitions. Unmarshalled to {@link #parts}. */
    private byte[] partsBytes;

    /** Partitions update counters. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Object> partCntrs;

    /** Serialized partitions counters. */
    private byte[] partCntrsBytes;

    /** Partitions sizes. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Map<Integer, Long>> partsSizes;

    /** Serialized partitions counters. */
    private byte[] partsSizesBytes;

    /** Partitions history reservation counters. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Map<Integer, Long>> partHistCntrs;

    /** Serialized partitions history reservation counters. */
    private byte[] partHistCntrsBytes;

    /** Exception. */
    @GridToStringInclude
    @GridDirectTransient
    private Exception err;

    /** */
    private byte[] errBytes;

    /** */
    private boolean client;

    /** */
    @GridDirectCollection(Integer.class)
    private Collection<Integer> grpsAffRequest;

    /** Start time of exchange on node which sent this message in nanoseconds. */
    private long exchangeStartTime;

    /**
     * Exchange finish message, sent to new coordinator when it tries to restore state after previous coordinator failed
     * during exchange.
     */
    private GridDhtPartitionsFullMessage finishMsg;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsSingleMessage() {
        // No-op.
    }

    /**
     * @param exchId Exchange ID.
     * @param client Client message flag.
     * @param lastVer Last version.
     * @param compress {@code True} if it is possible to use compression for message.
     */
    public GridDhtPartitionsSingleMessage(GridDhtPartitionExchangeId exchId,
        boolean client,
        @Nullable GridCacheVersion lastVer,
        boolean compress
    ) {
        super(exchId, lastVer);

        compressed(compress);

        this.client = client;
    }

    /**
     * @param finishMsg Exchange finish message (used to restore exchange state on new coordinator).
     */
    void finishMessage(GridDhtPartitionsFullMessage finishMsg) {
        this.finishMsg = finishMsg;
    }

    /**
     * @return Exchange finish message (used to restore exchange state on new coordinator).
     */
    GridDhtPartitionsFullMessage finishMessage() {
        return finishMsg;
    }

    /**
     * @param grpsAffRequest Cache groups to get affinity for (affinity is requested when node joins cluster).
     */
    void cacheGroupsAffinityRequest(Collection<Integer> grpsAffRequest) {
        this.grpsAffRequest = grpsAffRequest;
    }

    /**
     * @return Cache groups to get affinity for (affinity is requested when node joins cluster).
     */
    @Nullable public Collection<Integer> cacheGroupsAffinityRequest() {
        return grpsAffRequest;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /**
     * @return {@code True} if sent from client node.
     */
    public boolean client() {
        return client;
    }

    /**
     * @param cacheId Cache ID to add local partition for.
     * @param locMap Local partition map.
     * @param dupDataCache Optional ID of cache with the same partition state map.
     */
    public void addLocalPartitionMap(int cacheId, GridDhtPartitionMap locMap, @Nullable Integer dupDataCache) {
        if (parts == null)
            parts = new HashMap<>();

        parts.put(cacheId, locMap);

        if (dupDataCache != null) {
            assert compressed();
            assert F.isEmpty(locMap.map());
            assert parts.containsKey(dupDataCache);

            if (dupPartsData == null)
                dupPartsData = new HashMap<>();

            dupPartsData.put(cacheId, dupDataCache);
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param cntrMap Partition update counters.
     */
    public void addPartitionUpdateCounters(int grpId, Object cntrMap) {
        if (partCntrs == null)
            partCntrs = new HashMap<>();

        partCntrs.put(grpId, cntrMap);
    }

    /**
     * @param grpId Cache group ID.
     * @param partsCnt Total cache partitions.
     * @return Partition update counters.
     */
    public CachePartitionPartialCountersMap partitionUpdateCounters(int grpId, int partsCnt) {
        Object res = partCntrs == null ? null : partCntrs.get(grpId);

        if (res == null)
            return CachePartitionPartialCountersMap.EMPTY;

        if (res instanceof CachePartitionPartialCountersMap)
            return (CachePartitionPartialCountersMap)res;

        assert res instanceof Map : res;

        Map<Integer, T2<Long, Long>> map = (Map<Integer, T2<Long, Long>>)res;

        return CachePartitionPartialCountersMap.fromCountersMap(map, partsCnt);
    }

    /**
     * Adds partition sizes map for specified {@code grpId} to the current message.
     *
     * @param grpId Group id.
     * @param partSizesMap Partition sizes map.
     */
    public void addPartitionSizes(int grpId, Map<Integer, Long> partSizesMap) {
        if (partSizesMap.isEmpty())
            return;

        if (partsSizes == null)
            partsSizes = new HashMap<>();

        partsSizes.put(grpId, partSizesMap);
    }

    /**
     * Returns partition sizes map for specified {@code grpId}.
     *
     * @param grpId Group id.
     * @return Partition sizes map (partId, partSize).
     */
    public Map<Integer, Long> partitionSizes(int grpId) {
        if (partsSizes == null)
            return Collections.emptyMap();

        return partsSizes.getOrDefault(grpId, Collections.emptyMap());
    }

    /**
     * @param grpId Cache group ID.
     * @param cntrMap Partition history counters.
     */
    public void partitionHistoryCounters(int grpId, Map<Integer, Long> cntrMap) {
        if (cntrMap.isEmpty())
            return;

        if (partHistCntrs == null)
            partHistCntrs = new HashMap<>();

        partHistCntrs.put(grpId, cntrMap);
    }

    /**
     * @param cntrMap Partition history counters.
     */
    void partitionHistoryCounters(Map<Integer, Map<Integer, Long>> cntrMap) {
        for (Map.Entry<Integer, Map<Integer, Long>> e : cntrMap.entrySet())
            partitionHistoryCounters(e.getKey(), e.getValue());
    }

    /**
     * @param grpId Cache group ID.
     * @return Partition history counters.
     */
    Map<Integer, Long> partitionHistoryCounters(int grpId) {
        if (partHistCntrs != null) {
            Map<Integer, Long> res = partHistCntrs.get(grpId);

            return res != null ? res : Collections.<Integer, Long>emptyMap();
        }

        return Collections.emptyMap();
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionMap> partitions() {
        if (parts == null)
            parts = new HashMap<>();

        return parts;
    }

    /**
     * @param ex Exception.
     */
    public void setError(Exception ex) {
        this.err = ex;
    }

    /**
     * @return Not null exception if exchange processing failed.
     */
    @Nullable public Exception getError() {
        return err;
    }

    /**
     * Start time of exchange on node which sent this message.
     */
    public long exchangeStartTime() {
        return exchangeStartTime;
    }

    /**
     * @param exchangeStartTime Start time of exchange.
     */
    public void exchangeStartTime(long exchangeStartTime) {
        this.exchangeStartTime = exchangeStartTime;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        boolean marshal = (parts != null && partsBytes == null) ||
            (partCntrs != null && partCntrsBytes == null) ||
            (partHistCntrs != null && partHistCntrsBytes == null) ||
            (partsSizes != null && partsSizesBytes == null) ||
            (err != null && errBytes == null);

        if (marshal) {
            byte[] partsBytes0 = null;
            byte[] partCntrsBytes0 = null;
            byte[] partHistCntrsBytes0 = null;
            byte[] partsSizesBytes0 = null;
            byte[] errBytes0 = null;

            if (parts != null && partsBytes == null)
                partsBytes0 = U.marshal(ctx, parts);

            if (partCntrs != null && partCntrsBytes == null)
                partCntrsBytes0 = U.marshal(ctx, partCntrs);

            if (partHistCntrs != null && partHistCntrsBytes == null)
                partHistCntrsBytes0 = U.marshal(ctx, partHistCntrs);

            if (partsSizes != null && partsSizesBytes == null)
                partsSizesBytes0 = U.marshal(ctx, partsSizes);

            if (err != null && errBytes == null)
                errBytes0 = U.marshal(ctx, err);

            if (compressed()) {
                try {
                    byte[] partsBytesZip = U.zip(partsBytes0);
                    byte[] partCntrsBytesZip = U.zip(partCntrsBytes0);
                    byte[] partHistCntrsBytesZip = U.zip(partHistCntrsBytes0);
                    byte[] partsSizesBytesZip = U.zip(partsSizesBytes0);
                    byte[] exBytesZip = U.zip(errBytes0);

                    partsBytes0 = partsBytesZip;
                    partCntrsBytes0 = partCntrsBytesZip;
                    partHistCntrsBytes0 = partHistCntrsBytesZip;
                    partsSizesBytes0 = partsSizesBytesZip;
                    errBytes0 = exBytesZip;
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.logger(getClass()), "Failed to compress partitions data: " + e, e);
                }
            }

            partsBytes = partsBytes0;
            partCntrsBytes = partCntrsBytes0;
            partHistCntrsBytes = partHistCntrsBytes0;
            partsSizesBytes = partsSizesBytes0;
            errBytes = errBytes0;
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null && parts == null) {
            if (compressed())
                parts = U.unmarshalZip(ctx.marshaller(), partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                parts = U.unmarshal(ctx, partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partCntrsBytes != null && partCntrs == null) {
            if (compressed())
                partCntrs = U.unmarshalZip(ctx.marshaller(), partCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partCntrs = U.unmarshal(ctx, partCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partHistCntrsBytes != null && partHistCntrs == null) {
            if (compressed())
                partHistCntrs = U.unmarshalZip(ctx.marshaller(), partHistCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partHistCntrs = U.unmarshal(ctx, partHistCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partsSizesBytes != null && partsSizes == null) {
            if (compressed())
                partsSizes = U.unmarshalZip(ctx.marshaller(), partsSizesBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partsSizes = U.unmarshal(ctx, partsSizesBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (errBytes != null && err == null) {
            if (compressed())
                err = U.unmarshalZip(ctx.marshaller(), errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (dupPartsData != null) {
            assert parts != null;

            for (Map.Entry<Integer, Integer> e : dupPartsData.entrySet()) {
                GridDhtPartitionMap map1 = parts.get(e.getKey());

                assert map1 != null : e.getKey();
                assert F.isEmpty(map1.map());
                assert !map1.hasMovingPartitions();

                GridDhtPartitionMap map2 = parts.get(e.getValue());

                assert map2 != null : e.getValue();
                assert map2.map() != null;

                for (Map.Entry<Integer, GridDhtPartitionState> e0 : map2.map().entrySet())
                    map1.put(e0.getKey(), e0.getValue());
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
            case 6:
                if (!writer.writeBoolean("client", client))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap("dupPartsData", dupPartsData, MessageCollectionItemType.INT, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeLong("exchangeStartTime", exchangeStartTime))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("finishMsg", finishMsg))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection("grpsAffRequest", grpsAffRequest, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeByteArray("partCntrsBytes", partCntrsBytes))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByteArray("partHistCntrsBytes", partHistCntrsBytes))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByteArray("partsBytes", partsBytes))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeByteArray("partsSizesBytes", partsSizesBytes))
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
            case 6:
                client = reader.readBoolean("client");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                dupPartsData = reader.readMap("dupPartsData", MessageCollectionItemType.INT, MessageCollectionItemType.INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                exchangeStartTime = reader.readLong("exchangeStartTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                finishMsg = reader.readMessage("finishMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                grpsAffRequest = reader.readCollection("grpsAffRequest", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                partCntrsBytes = reader.readByteArray("partCntrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                partHistCntrsBytes = reader.readByteArray("partHistCntrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                partsBytes = reader.readByteArray("partsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                partsSizesBytes = reader.readByteArray("partsSizesBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionsSingleMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 47;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsSingleMessage.class, this, super.toString());
    }
}
