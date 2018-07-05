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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of all nodes in topology.
 */
public class GridDhtPartitionsFullMessage extends GridDhtPartitionsAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** grpId -> FullMap */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, GridDhtPartitionFullMap> parts;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = Integer.class)
    private Map<Integer, Integer> dupPartsData;

    /** */
    private byte[] partsBytes;

    /** Partitions update counters. */
    @GridToStringInclude
    @GridDirectTransient
    private IgniteDhtPartitionCountersMap partCntrs;

    /** Serialized partitions counters. */
    private byte[] partCntrsBytes;

    /** Partitions update counters. */
    @GridToStringInclude
    @GridDirectTransient
    private IgniteDhtPartitionCountersMap2 partCntrs2;

    /** Serialized partitions counters. */
    private byte[] partCntrsBytes2;

    /** Partitions history suppliers. */
    @GridToStringInclude
    @GridDirectTransient
    private IgniteDhtPartitionHistorySuppliersMap partHistSuppliers;

    /** Serialized partitions history suppliers. */
    private byte[] partHistSuppliersBytes;

    /** Partitions that must be cleared and re-loaded. */
    @GridToStringInclude
    @GridDirectTransient
    private IgniteDhtPartitionsToReloadMap partsToReload;

    /** Serialized partitions that must be cleared and re-loaded. */
    private byte[] partsToReloadBytes;

    /** Partitions sizes. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Map<Integer, Long>> partsSizes;

    /** Serialized partitions sizes. */
    private byte[] partsSizesBytes;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Exceptions. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<UUID, Exception> errs;

    /**  */
    private byte[] errsBytes;

    /** */
    @GridDirectTransient
    private transient boolean compress;

    /** */
    private AffinityTopologyVersion resTopVer;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = CacheGroupAffinityMessage.class)
    private Map<Integer, CacheGroupAffinityMessage> joinedNodeAff;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = CacheGroupAffinityMessage.class)
    private Map<Integer, CacheGroupAffinityMessage> idealAffDiff;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsFullMessage() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     * @param lastVer Last version.
     * @param topVer Topology version. For messages not related to exchange may be {@link AffinityTopologyVersion#NONE}.
     * @param partHistSuppliers
     * @param partsToReload
     */
    public GridDhtPartitionsFullMessage(@Nullable GridDhtPartitionExchangeId id,
        @Nullable GridCacheVersion lastVer,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable IgniteDhtPartitionHistorySuppliersMap partHistSuppliers,
        @Nullable IgniteDhtPartitionsToReloadMap partsToReload) {
        super(id, lastVer);

        assert id == null || topVer.equals(id.topologyVersion());

        this.topVer = topVer;
        this.partHistSuppliers = partHistSuppliers;
        this.partsToReload = partsToReload;
    }

    /** {@inheritDoc} */
    @Override void copyStateTo(GridDhtPartitionsAbstractMessage msg) {
        super.copyStateTo(msg);

        GridDhtPartitionsFullMessage cp = (GridDhtPartitionsFullMessage)msg;

        cp.parts = parts;
        cp.dupPartsData = dupPartsData;
        cp.partsBytes = partsBytes;
        cp.partCntrs = partCntrs;
        cp.partCntrsBytes = partCntrsBytes;
        cp.partCntrs2 = partCntrs2;
        cp.partCntrsBytes2 = partCntrsBytes2;
        cp.partHistSuppliers = partHistSuppliers;
        cp.partHistSuppliersBytes = partHistSuppliersBytes;
        cp.partsToReload = partsToReload;
        cp.partsToReloadBytes = partsToReloadBytes;
        cp.partsSizes = partsSizes;
        cp.partsSizesBytes = partsSizesBytes;
        cp.topVer = topVer;
        cp.errs = errs;
        cp.errsBytes = errsBytes;
        cp.compress = compress;
        cp.resTopVer = resTopVer;
        cp.joinedNodeAff = joinedNodeAff;
        cp.idealAffDiff = idealAffDiff;
    }

    /**
     * @return Message copy.
     */
    GridDhtPartitionsFullMessage copy() {
        GridDhtPartitionsFullMessage cp = new GridDhtPartitionsFullMessage();

        copyStateTo(cp);

        return cp;
    }

    /**
     * @param resTopVer Result topology version.
     */
    public void resultTopologyVersion(AffinityTopologyVersion resTopVer) {
        this.resTopVer = resTopVer;
    }

    /**
     * @return Result topology version.
     */
    public AffinityTopologyVersion resultTopologyVersion() {
        return resTopVer;
    }

    /**
     * @return Caches affinity for joining nodes.
     */
    @Nullable public Map<Integer, CacheGroupAffinityMessage> joinedNodeAffinity() {
        return joinedNodeAff;
    }

    /**
     * @param joinedNodeAff Caches affinity for joining nodes.
     */
    void joinedNodeAffinity(Map<Integer, CacheGroupAffinityMessage> joinedNodeAff) {
        this.joinedNodeAff = joinedNodeAff;
    }

    /**
     * @return Difference with ideal affinity.
     */
    @Nullable public Map<Integer, CacheGroupAffinityMessage> idealAffinityDiff() {
        return idealAffDiff;
    }

    /**
     * @param idealAffDiff Difference with ideal affinity.
     */
    void idealAffinityDiff(Map<Integer, CacheGroupAffinityMessage> idealAffDiff) {
        this.idealAffDiff = idealAffDiff;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /**
     * @param compress {@code True} if it is possible to use compression for message.
     */
    public void compress(boolean compress) {
        this.compress = compress;
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionFullMap> partitions() {
        if (parts == null)
            parts = new HashMap<>();

        return parts;
    }

    /**
     * @param grpId Cache group ID.
     * @return {@code True} if message contains full map for given cache.
     */
    public boolean containsGroup(int grpId) {
        return parts != null && parts.containsKey(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @param fullMap Full partitions map.
     * @param dupDataCache Optional ID of cache with the same partition state map.
     */
    public void addFullPartitionsMap(int grpId, GridDhtPartitionFullMap fullMap, @Nullable Integer dupDataCache) {
        assert fullMap != null;

        if (parts == null)
            parts = new HashMap<>();

        if (!parts.containsKey(grpId)) {
            parts.put(grpId, fullMap);

            if (dupDataCache != null) {
                assert compress;
                assert parts.containsKey(dupDataCache);

                if (dupPartsData == null)
                    dupPartsData = new HashMap<>();

                dupPartsData.put(grpId, dupDataCache);
            }
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param cntrMap Partition update counters.
     */
    public void addPartitionUpdateCounters(int grpId, Map<Integer, T2<Long, Long>> cntrMap) {
        if (partCntrs == null)
            partCntrs = new IgniteDhtPartitionCountersMap();

        partCntrs.putIfAbsent(grpId, cntrMap);
    }

    /**
     * @param grpId Cache group ID.
     * @param cntrMap Partition update counters.
     */
    public void addPartitionUpdateCounters(int grpId, CachePartitionFullCountersMap cntrMap) {
        if (partCntrs2 == null)
            partCntrs2 = new IgniteDhtPartitionCountersMap2();

        partCntrs2.putIfAbsent(grpId, cntrMap);
    }

    /**
     * @param grpId Cache group ID.
     * @param partsCnt Total cache partitions.
     * @return Partition update counters.
     */
    public CachePartitionFullCountersMap partitionUpdateCounters(int grpId, int partsCnt) {
        if (partCntrs2 != null)
            return partCntrs2.get(grpId);

        if (partCntrs == null)
            return null;

        Map<Integer, T2<Long, Long>> map = partCntrs.get(grpId);

        return map != null ? CachePartitionFullCountersMap.fromCountersMap(map, partsCnt) : null;
    }

    /**
     *
     */
    public IgniteDhtPartitionHistorySuppliersMap partitionHistorySuppliers() {
        if (partHistSuppliers == null)
            return IgniteDhtPartitionHistorySuppliersMap.empty();

        return partHistSuppliers;
    }

    /**
     *
     */
    public Set<Integer> partsToReload(UUID nodeId, int grpId) {
        if (partsToReload == null)
            return Collections.emptySet();

        return partsToReload.get(nodeId, grpId);
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
     * @return Errors map.
     */
    @Nullable Map<UUID, Exception> getErrorsMap() {
        return errs;
    }

    /**
     * @param errs Errors map.
     */
    void setErrorsMap(Map<UUID, Exception> errs) {
        this.errs = new HashMap<>(errs);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        boolean marshal = (!F.isEmpty(parts) && partsBytes == null) ||
            (partCntrs != null && !partCntrs.empty() && partCntrsBytes == null) ||
            (partCntrs2 != null && !partCntrs2.empty() && partCntrsBytes2 == null) ||
            (partHistSuppliers != null && partHistSuppliersBytes == null) ||
            (partsToReload != null && partsToReloadBytes == null) ||
            (!F.isEmpty(errs) && errsBytes == null);

        if (marshal) {
            byte[] partsBytes0 = null;
            byte[] partCntrsBytes0 = null;
            byte[] partCntrsBytes20 = null;
            byte[] partHistSuppliersBytes0 = null;
            byte[] partsToReloadBytes0 = null;
            byte[] partsSizesBytes0 = null;
            byte[] errsBytes0 = null;

            if (!F.isEmpty(parts) && partsBytes == null)
                partsBytes0 = U.marshal(ctx, parts);

            if (partCntrs != null && !partCntrs.empty() && partCntrsBytes == null)
                partCntrsBytes0 = U.marshal(ctx, partCntrs);

            if (partCntrs2 != null && !partCntrs2.empty() && partCntrsBytes2 == null)
                partCntrsBytes20 = U.marshal(ctx, partCntrs2);

            if (partHistSuppliers != null && partHistSuppliersBytes == null)
                partHistSuppliersBytes0 = U.marshal(ctx, partHistSuppliers);

            if (partsToReload != null && partsToReloadBytes == null)
                partsToReloadBytes0 = U.marshal(ctx, partsToReload);

            if (partsSizes != null && partsSizesBytes == null)
                partsSizesBytes0 = U.marshal(ctx, partsSizes);

            if (!F.isEmpty(errs) && errsBytes == null)
                errsBytes0 = U.marshal(ctx, errs);

            if (compress) {
                assert !compressed();

                try {
                    byte[] partsBytesZip = U.zip(partsBytes0);
                    byte[] partCntrsBytesZip = U.zip(partCntrsBytes0);
                    byte[] partCntrsBytes2Zip = U.zip(partCntrsBytes20);
                    byte[] partHistSuppliersBytesZip = U.zip(partHistSuppliersBytes0);
                    byte[] partsToReloadBytesZip = U.zip(partsToReloadBytes0);
                    byte[] partsSizesBytesZip = U.zip(partsSizesBytes0);
                    byte[] exsBytesZip = U.zip(errsBytes0);

                    partsBytes0 = partsBytesZip;
                    partCntrsBytes0 = partCntrsBytesZip;
                    partCntrsBytes20 = partCntrsBytes2Zip;
                    partHistSuppliersBytes0 = partHistSuppliersBytesZip;
                    partsToReloadBytes0 = partsToReloadBytesZip;
                    partsSizesBytes0 = partsSizesBytesZip;
                    errsBytes0 = exsBytesZip;

                    compressed(true);
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.logger(getClass()), "Failed to compress partitions data: " + e, e);
                }
            }

            partsBytes = partsBytes0;
            partCntrsBytes = partCntrsBytes0;
            partCntrsBytes2 = partCntrsBytes20;
            partHistSuppliersBytes = partHistSuppliersBytes0;
            partsToReloadBytes = partsToReloadBytes0;
            partsSizesBytes = partsSizesBytes0;
            errsBytes = errsBytes0;
        }
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null && parts == null) {
            if (compressed())
                parts = U.unmarshalZip(ctx.marshaller(), partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                parts = U.unmarshal(ctx, partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

            if (dupPartsData != null) {
                assert parts != null;

                for (Map.Entry<Integer, Integer> e : dupPartsData.entrySet()) {
                    GridDhtPartitionFullMap map1 = parts.get(e.getKey());
                    GridDhtPartitionFullMap map2 = parts.get(e.getValue());

                    assert map1 != null : e.getKey();
                    assert map2 != null : e.getValue();
                    assert map1.size() == map2.size();

                    for (Map.Entry<UUID, GridDhtPartitionMap> e0 : map2.entrySet()) {
                        GridDhtPartitionMap partMap1 = map1.get(e0.getKey());

                        assert partMap1 != null && partMap1.map().isEmpty() : partMap1;
                        assert !partMap1.hasMovingPartitions() : partMap1;

                        GridDhtPartitionMap partMap2 = e0.getValue();

                        assert partMap2 != null;

                        for (Map.Entry<Integer, GridDhtPartitionState> stateEntry : partMap2.entrySet())
                            partMap1.put(stateEntry.getKey(), stateEntry.getValue());
                    }
                }
            }
        }

        if (parts == null)
            parts = new HashMap<>();

        if (partCntrsBytes != null && partCntrs == null) {
            if (compressed())
                partCntrs = U.unmarshalZip(ctx.marshaller(), partCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partCntrs = U.unmarshal(ctx, partCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partCntrsBytes2 != null && partCntrs2 == null) {
            if (compressed())
                partCntrs2 = U.unmarshalZip(ctx.marshaller(), partCntrsBytes2, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partCntrs2 = U.unmarshal(ctx, partCntrsBytes2, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partHistSuppliersBytes != null && partHistSuppliers == null) {
            if (compressed())
                partHistSuppliers = U.unmarshalZip(ctx.marshaller(), partHistSuppliersBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partHistSuppliers = U.unmarshal(ctx, partHistSuppliersBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partsToReloadBytes != null && partsToReload == null) {
            if (compressed())
                partsToReload = U.unmarshalZip(ctx.marshaller(), partsToReloadBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partsToReload = U.unmarshal(ctx, partsToReloadBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partsSizesBytes != null && partsSizes == null) {
            if (compressed())
                partsSizes = U.unmarshalZip(ctx.marshaller(), partsSizesBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partsSizes = U.unmarshal(ctx, partsSizesBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partCntrs == null)
            partCntrs = new IgniteDhtPartitionCountersMap();

        if (errsBytes != null && errs == null) {
            if (compressed())
                errs = U.unmarshalZip(ctx.marshaller(), errsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                errs = U.unmarshal(ctx, errsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (errs == null)
            errs = new HashMap<>();
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
            case 5:
                if (!writer.writeMap("dupPartsData", dupPartsData, MessageCollectionItemType.INT, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByteArray("errsBytes", errsBytes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap("idealAffDiff", idealAffDiff, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap("joinedNodeAff", joinedNodeAff, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByteArray("partCntrsBytes", partCntrsBytes))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByteArray("partCntrsBytes2", partCntrsBytes2))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByteArray("partHistSuppliersBytes", partHistSuppliersBytes))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeByteArray("partsBytes", partsBytes))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByteArray("partsSizesBytes", partsSizesBytes))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByteArray("partsToReloadBytes", partsToReloadBytes))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("resTopVer", resTopVer))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMessage("topVer", topVer))
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
            case 5:
                dupPartsData = reader.readMap("dupPartsData", MessageCollectionItemType.INT, MessageCollectionItemType.INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                errsBytes = reader.readByteArray("errsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                idealAffDiff = reader.readMap("idealAffDiff", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                joinedNodeAff = reader.readMap("joinedNodeAff", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                partCntrsBytes = reader.readByteArray("partCntrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                partCntrsBytes2 = reader.readByteArray("partCntrsBytes2");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                partHistSuppliersBytes = reader.readByteArray("partHistSuppliersBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                partsBytes = reader.readByteArray("partsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                partsSizesBytes = reader.readByteArray("partsSizesBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                partsToReloadBytes = reader.readByteArray("partsToReloadBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                resTopVer = reader.readMessage("resTopVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionsFullMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 46;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsFullMessage.class, this, "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}
