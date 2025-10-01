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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of all nodes in topology. <br> Is sent by topology coordinator: when all {@link
 * GridDhtPartitionsSingleMessage}s were received. <br> May be also compacted as part of {@link
 * CacheAffinityChangeMessage} for node left or failed case.<br>
 */
public class GridDhtPartitionsFullMessage extends GridDhtPartitionsAbstractMessage {
    /** */
    private static final byte REBALANCED_FLAG_MASK = 0x01;

    /** grpId -> FullMap */
    @GridToStringInclude
    private Map<Integer, GridDhtPartitionFullMap> parts;

    /** Partitions update counters. */
    @GridToStringInclude
    private IgniteDhtPartitionCountersMap partCntrs;

    /** Partitions history suppliers. */
    @GridToStringInclude
    private IgniteDhtPartitionHistorySuppliersMap partHistSuppliers;

    /** Partitions that must be cleared and re-loaded. */
    @GridToStringInclude
    private IgniteDhtPartitionsToReloadMap partsToReload;

    /** Exceptions. */
    @GridToStringInclude
    private Map<UUID, Exception> errs;

    /** */
    @Order(value = 6, method = "duplicatedPartitionsData")
    private Map<Integer, Integer> dupPartsData;

    /**  */
    @Order(value = 7, method = "errorBytes")
    private byte[] errsBytes;

    /** */
    @Order(8)
    private byte flags;

    /** */
    @Order(value = 9, method = "idealAffinityDiff")
    private Map<Integer, CacheGroupAffinityMessage> idealAffDiff;

    /** */
    @Order(value = 10, method = "joinedNodeAffinity")
    @Nullable private Map<Integer, CacheGroupAffinityMessage> joinedNodeAff;

    /** */
    @Order(value = 11, method = "lostPartitions")
    @GridToStringExclude
    private Map<Integer, int[]> lostParts;

    /** Serialized partitions counters. */
    @Order(value = 12, method = "partitionCounterBytes")
    private byte[] partCntrsBytes;

    /** Serialized partitions history suppliers. */
    @Order(value = 13, method = "partitionHistorySupplierBytes")
    private byte[] partHistSuppliersBytes;

    /** */
    @Order(value = 14, method = "partitionBytes")
    private byte[] partsBytes;

    /** Serialized partitions sizes. */
    @Order(value = 15, method = "partitionSizesBytes")
    private byte[] partsSizesBytes;

    /** Serialized partitions that must be cleared and re-loaded. */
    @Order(value = 16, method = "partitionsToReloadBytes")
    private byte[] partsToReloadBytes;

    /** */
    @Order(value = 17, method = "resultTopologyVersion")
    private AffinityTopologyVersion resTopVer;

    /** Topology version. */
    @Order(value = 18, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /**
     * Empty constructor.
     */
    public GridDhtPartitionsFullMessage() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     * @param lastVer Last version.
     * @param topVer Topology version. For messages not related to exchange may be {@link AffinityTopologyVersion#NONE}.
     * @param partHistSuppliers Suppliers.
     * @param partsToReload Partitions to reload.
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

        if (parts != null) {
            cp.parts = new HashMap<>(parts.size());

            for (Map.Entry<Integer, GridDhtPartitionFullMap> e : parts.entrySet()) {
                GridDhtPartitionFullMap val = e.getValue();

                cp.parts.put(e.getKey(), new GridDhtPartitionFullMap(
                    val.nodeId(),
                    val.nodeOrder(),
                    val.updateSequence(),
                    val,
                    false));
            }
        }
        else
            cp.parts = null;

        cp.dupPartsData = dupPartsData;
        cp.partsBytes = partsBytes;
        cp.partCntrs = partCntrs;
        cp.partCntrsBytes = partCntrsBytes;
        cp.partHistSuppliers = partHistSuppliers;
        cp.partHistSuppliersBytes = partHistSuppliersBytes;
        cp.partsToReload = partsToReload;
        cp.partsToReloadBytes = partsToReloadBytes;
        cp.partsSizesBytes = partsSizesBytes;
        cp.topVer = topVer;
        cp.errs = errs;
        cp.errsBytes = errsBytes;
        cp.resTopVer = resTopVer;
        cp.joinedNodeAff = joinedNodeAff;
        cp.idealAffDiff = idealAffDiff;
        cp.flags = flags;
        cp.lostParts = lostParts;
    }

    /**
     * @return Message copy.
     */
    public GridDhtPartitionsFullMessage copy() {
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
    public GridDhtPartitionsFullMessage joinedNodeAffinity(Map<Integer, CacheGroupAffinityMessage> joinedNodeAff) {
        this.joinedNodeAff = joinedNodeAff;

        return this;
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
    public void idealAffinityDiff(Map<Integer, CacheGroupAffinityMessage> idealAffDiff) {
        this.idealAffDiff = idealAffDiff;
    }

    /** Sets the duplicated data. */
    public void duplicatedPartitionsData(Map<Integer, Integer> dupPartsData) {
        this.dupPartsData = dupPartsData;
    }

    /** @return the duplicated data. */
    public Map<Integer, Integer> duplicatedPartitionsData() {
        return dupPartsData;
    }

    /**  */
    public void errorBytes(byte[] errBytes) {
        this.errsBytes = errBytes;
    }

    /** */
    public byte[] errorBytes() {
        return errsBytes;
    }

    /** @return  the lost partitions. */
    public Map<Integer, int[]> lostPartitions() {
        return lostParts;
    }

    /** Sets the serialized partitions counters. */
    public void partitionCounterBytes(byte[] partCntrsBytes) {
        this.partCntrsBytes = partCntrsBytes;
    }

    /** @return the serialized partitions counters. */
    public byte[] partitionCounterBytes() {
        return partCntrsBytes;
    }

    /** Sets the lost partitions. */
    public void lostPartitions(Map<Integer, int[]> lostParts) {
        this.lostParts = lostParts;
    }

    /** Sets the serialized partitions history suppliers. */
    public void partitionHistorySupplierBytes(byte[] partHistSuppliersBytes) {
        this.partHistSuppliersBytes = partHistSuppliersBytes;
    }

    /** @return The serialized partitions history suppliers. */
    public byte[] partitionHistorySupplierBytes() {
        return partHistSuppliersBytes;
    }

    /** */
    public void partitionBytes(byte[] partsBytes) {
        this.partsBytes = partsBytes;
    }

    /** @return the serialized partitions counters. */
    public byte[] partitionBytes() {
        return partsBytes;
    }

    /** Sets the serialized partitions sizes. */
    public void partitionSizesBytes(byte[] partsSizesBytes) {
        this.partsSizesBytes = partsSizesBytes;
    }

    /** @return The serialized partitions sizes. */
    public byte[] partitionSizesBytes() {
        return partsSizesBytes;
    }

    /** @return The serialized partitions that must be cleared and re-loaded. */
    public byte[] partitionsToReloadBytes() {
        return partsToReloadBytes;
    }

    /** Sets the serialized partitions that must be cleared and re-loaded. */
    public void partitionsToReloadBytes(byte[] partsToReloadBytes) {
        this.partsToReloadBytes = partsToReloadBytes;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
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
                assert compressed();
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
    public void addPartitionUpdateCounters(int grpId, CachePartitionFullCountersMap cntrMap) {
        if (partCntrs == null)
            partCntrs = new IgniteDhtPartitionCountersMap();

        partCntrs.putIfAbsent(grpId, cntrMap);
    }

    /**
     * @param grpId Group id.
     * @param lostParts Lost parts.
     */
    public void addLostPartitions(int grpId, Collection<Integer> lostParts) {
        if (lostParts.isEmpty())
            return;

        if (this.lostParts == null)
            this.lostParts = new HashMap<>();

        this.lostParts.put(grpId, lostParts.stream().mapToInt(v -> v).toArray());
    }

    /**
     * @param grpId Group id.
     * @return Lost partitions for a group.
     */
    @Nullable public Set<Integer> lostPartitions(int grpId) {
        if (lostParts == null)
            return null;

        int[] parts = lostParts.get(grpId);

        if (parts == null)
            return Collections.emptySet();

        return IntStream.of(parts).boxed().collect(Collectors.toSet());
    }

    /**
     * @param grpId Cache group ID.
     * @return Partition update counters.
     */
    public CachePartitionFullCountersMap partitionUpdateCounters(int grpId) {
        return partCntrs == null ? null : partCntrs.get(grpId);
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
     * Supplies partition sizes map for all cache groups.
     *
     * @param ctx Cache context.
     * @param partsSizes Partitions sizes map.
     */
    public void partitionSizes(GridCacheSharedContext ctx, Map<Integer, Map<Integer, Long>> partsSizes) {
        try {
            byte[] marshalled = U.marshal(ctx, partsSizes);

            if (compressed())
                marshalled = U.zip(marshalled, ctx.gridConfig().getNetworkCompressionLevel());

            partsSizesBytes = marshalled;
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }
    }

    /**
     * Returns partition sizes map for all cache groups.
     *
     * @param ctx Cache context.
     * @return Partition sizes map (grpId, (partId, partSize)).
     */
    public Map<Integer, Map<Integer, Long>> partitionSizes(GridCacheSharedContext ctx) {
        if (partsSizesBytes == null)
            return Collections.emptyMap();

        try {
            return compressed()
                ? U.unmarshalZip(ctx.marshaller(), partsSizesBytes, ctx.deploy().globalLoader())
                : U.unmarshal(ctx, partsSizesBytes, ctx.deploy().globalLoader());
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }
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

    /**
     * Rebalance finished.
     */
    public boolean rebalanced() {
        return (flags & REBALANCED_FLAG_MASK) != 0;
    }

    /**
     * @param rebalanced {@code True} if data is fully rebalanced.
     */
    public void rebalanced(boolean rebalanced) {
        flags = rebalanced ? (byte)(flags | REBALANCED_FLAG_MASK) : (byte)(flags & ~REBALANCED_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        boolean marshal = (!F.isEmpty(parts) && partsBytes == null) ||
            (partCntrs != null && !partCntrs.empty() && partCntrsBytes == null) ||
            (partHistSuppliers != null && partHistSuppliersBytes == null) ||
            (partsToReload != null && partsToReloadBytes == null) ||
            (!F.isEmpty(errs) && errsBytes == null);

        if (marshal) {
            // Reserve at least 2 threads for system operations.
            int parallelismLvl = U.availableThreadCount(ctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2);

            Collection<Object> objectsToMarshall = new ArrayList<>();

            if (!F.isEmpty(parts) && partsBytes == null)
                objectsToMarshall.add(parts);

            if (partCntrs != null && !partCntrs.empty() && partCntrsBytes == null)
                objectsToMarshall.add(partCntrs);

            if (partHistSuppliers != null && partHistSuppliersBytes == null)
                objectsToMarshall.add(partHistSuppliers);

            if (partsToReload != null && partsToReloadBytes == null)
                objectsToMarshall.add(partsToReload);

            if (!F.isEmpty(errs) && errsBytes == null)
                objectsToMarshall.add(errs);

            Collection<byte[]> marshalled = U.doInParallel(
                parallelismLvl,
                ctx.kernalContext().pools().getSystemExecutorService(),
                objectsToMarshall,
                new IgniteThrowableFunction<Object, byte[]>() {
                    @Override public byte[] apply(Object payload) throws IgniteCheckedException {
                        byte[] marshalled = U.marshal(ctx, payload);

                        if (compressed())
                            marshalled = U.zip(marshalled, ctx.gridConfig().getNetworkCompressionLevel());

                        return marshalled;
                    }
                });

            Iterator<byte[]> iter = marshalled.iterator();

            if (!F.isEmpty(parts) && partsBytes == null)
                partsBytes = iter.next();

            if (partCntrs != null && !partCntrs.empty() && partCntrsBytes == null)
                partCntrsBytes = iter.next();

            if (partHistSuppliers != null && partHistSuppliersBytes == null)
                partHistSuppliersBytes = iter.next();

            if (partsToReload != null && partsToReloadBytes == null)
                partsToReloadBytes = iter.next();

            if (!F.isEmpty(errs) && errsBytes == null)
                errsBytes = iter.next();
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

        ClassLoader clsLdr = U.resolveClassLoader(ldr, ctx.gridConfig());

        Collection<byte[]> objectsToUnmarshall = new ArrayList<>();

        // Reserve at least 2 threads for system operations.
        int parallelismLvl = U.availableThreadCount(ctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2);

        if (partsBytes != null && parts == null)
            objectsToUnmarshall.add(partsBytes);

        if (partCntrsBytes != null && partCntrs == null)
            objectsToUnmarshall.add(partCntrsBytes);

        if (partHistSuppliersBytes != null && partHistSuppliers == null)
            objectsToUnmarshall.add(partHistSuppliersBytes);

        if (partsToReloadBytes != null && partsToReload == null)
            objectsToUnmarshall.add(partsToReloadBytes);

        if (errsBytes != null && errs == null)
            objectsToUnmarshall.add(errsBytes);

        Collection<Object> unmarshalled = U.doInParallel(
            parallelismLvl,
            ctx.kernalContext().pools().getSystemExecutorService(),
            objectsToUnmarshall,
            new IgniteThrowableFunction<byte[], Object>() {
                @Override public Object apply(byte[] binary) throws IgniteCheckedException {
                    return compressed()
                        ? U.unmarshalZip(ctx.marshaller(), binary, clsLdr)
                        : U.unmarshal(ctx, binary, clsLdr);
                }
            }
        );

        Iterator<Object> iter = unmarshalled.iterator();

        if (partsBytes != null && parts == null) {
            parts = (Map<Integer, GridDhtPartitionFullMap>)iter.next();

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

        if (partCntrsBytes != null && partCntrs == null)
            partCntrs = (IgniteDhtPartitionCountersMap)iter.next();

        if (partHistSuppliersBytes != null && partHistSuppliers == null)
            partHistSuppliers = (IgniteDhtPartitionHistorySuppliersMap)iter.next();

        if (partsToReloadBytes != null && partsToReload == null)
            partsToReload = (IgniteDhtPartitionsToReloadMap)iter.next();

        if (errsBytes != null && errs == null)
            errs = (Map<UUID, Exception>)iter.next();

        if (parts == null)
            parts = new HashMap<>();

        if (partCntrs == null)
            partCntrs = new IgniteDhtPartitionCountersMap();

        if (partHistSuppliers == null)
            partHistSuppliers = new IgniteDhtPartitionHistorySuppliersMap();

        if (partsToReload == null)
            partsToReload = new IgniteDhtPartitionsToReloadMap();

        if (errs == null)
            errs = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 46;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsFullMessage.class, this, "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }

    /**
     * Merges (replaces with newer) partitions map from given {@code other} full message.
     *
     * @param other Other full message.
     */
    public void merge(GridDhtPartitionsFullMessage other, GridDiscoveryManager discovery) {
        assert other.exchangeId() == null && exchangeId() == null :
            "Both current and merge full message must have exchangeId == null"
                + other.exchangeId() + "," + exchangeId();

        for (Map.Entry<Integer, GridDhtPartitionFullMap> grpAndMap : other.partitions().entrySet()) {
            int grpId = grpAndMap.getKey();
            GridDhtPartitionFullMap updMap = grpAndMap.getValue();

            GridDhtPartitionFullMap currMap = partitions().get(grpId);

            if (currMap == null)
                partitions().put(grpId, updMap);
            else {
                ClusterNode curMapSentBy = discovery.node(currMap.nodeId());
                ClusterNode newMapSentBy = discovery.node(updMap.nodeId());

                if (newMapSentBy == null)
                    continue;

                if (curMapSentBy == null || newMapSentBy.order() > curMapSentBy.order() || updMap.compareTo(currMap) >= 0)
                    partitions().put(grpId, updMap);
            }
        }
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        partsBytes = null;
        partCntrs = null;
        partCntrsBytes = null;
        partHistSuppliersBytes = null;
        partsToReloadBytes = null;
        partsSizesBytes = null;
        errsBytes = null;
    }
}
