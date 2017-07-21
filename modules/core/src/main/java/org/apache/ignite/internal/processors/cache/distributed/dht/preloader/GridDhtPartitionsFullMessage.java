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

    /** */
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

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsFullMessage() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     * @param lastVer Last version.
     * @param topVer Topology version.
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
     * @return Partition update counters.
     */
    @Override public Map<Integer, T2<Long, Long>> partitionUpdateCounters(int grpId) {
        if (partCntrs != null) {
            Map<Integer, T2<Long, Long>> res = partCntrs.get(grpId);

            return res != null ? res : Collections.<Integer, T2<Long, Long>>emptyMap();
        }

        return Collections.emptyMap();
    }

    /**
     *
     */
    public IgniteDhtPartitionHistorySuppliersMap partitionHistorySuppliers() {
        if (partHistSuppliers == null)
            return IgniteDhtPartitionHistorySuppliersMap.empty();

        return partHistSuppliers;
    }

    public Set<Integer> partsToReload(UUID nodeId, int grpId) {
        if (partsToReload == null)
            return Collections.emptySet();

        return partsToReload.get(nodeId, grpId);
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

        boolean marshal = (parts != null && partsBytes == null) ||
            (partCntrs != null && partCntrsBytes == null) ||
            (partHistSuppliers != null && partHistSuppliersBytes == null) ||
            (partsToReload != null && partsToReloadBytes == null) ||
            (errs != null && errsBytes == null);

        if (marshal) {
            byte[] partsBytes0 = null;
            byte[] partCntrsBytes0 = null;
            byte[] partHistSuppliersBytes0 = null;
            byte[] partsToReloadBytes0 = null;
            byte[] errsBytes0 = null;

            if (parts != null && partsBytes == null)
                partsBytes0 = U.marshal(ctx, parts);

            if (partCntrs != null && partCntrsBytes == null)
                partCntrsBytes0 = U.marshal(ctx, partCntrs);

            if (partHistSuppliers != null && partHistSuppliersBytes == null)
                partHistSuppliersBytes0 = U.marshal(ctx, partHistSuppliers);

            if (partsToReload != null && partsToReloadBytes == null)
                partsToReloadBytes0 = U.marshal(ctx, partsToReload);

            if (errs != null && errsBytes == null)
                errsBytes0 = U.marshal(ctx, errs);

            if (compress) {
                assert !compressed();

                try {
                    byte[] partsBytesZip = U.zip(partsBytes0);
                    byte[] partCntrsBytesZip = U.zip(partCntrsBytes0);
                    byte[] partHistSuppliersBytesZip = U.zip(partHistSuppliersBytes0);
                    byte[] partsToReloadBytesZip = U.zip(partsToReloadBytes0);
                    byte[] exsBytesZip = U.zip(errsBytes0);

                    partsBytes0 = partsBytesZip;
                    partCntrsBytes0 = partCntrsBytesZip;
                    partHistSuppliersBytes0 = partHistSuppliersBytesZip;
                    partsToReloadBytes0 = partsToReloadBytesZip;
                    errsBytes0 = exsBytesZip;

                    compressed(true);
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.logger(getClass()), "Failed to compress partitions data: " + e, e);
                }
            }

            partsBytes = partsBytes0;
            partCntrsBytes = partCntrsBytes0;
            partHistSuppliersBytes = partHistSuppliersBytes0;
            partsToReloadBytes = partsToReloadBytes0;
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
                if (!writer.writeByteArray("partCntrsBytes", partCntrsBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByteArray("partHistSuppliersBytes", partHistSuppliersBytes))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByteArray("partsBytes", partsBytes))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByteArray("partsToReloadBytes", partsToReloadBytes))
                    return false;

                writer.incrementState();

            case 11:
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
                partCntrsBytes = reader.readByteArray("partCntrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                partHistSuppliersBytes = reader.readByteArray("partHistSuppliersBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                partsBytes = reader.readByteArray("partsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                partsToReloadBytes = reader.readByteArray("partsToReloadBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
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
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsFullMessage.class, this, "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}
