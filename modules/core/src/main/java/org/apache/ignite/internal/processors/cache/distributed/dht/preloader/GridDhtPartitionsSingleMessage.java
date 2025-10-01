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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of a single node. <br>
 *
 * Sent in response to {@link GridDhtPartitionsSingleRequest} and during processing partitions exchange future.
 */
public class GridDhtPartitionsSingleMessage extends GridDhtPartitionsAbstractMessage {
    /** Local partitions. Serialized as {@link #partsBytes}, may be compressed. */
    @GridToStringInclude
    private Map<Integer, GridDhtPartitionMap> parts;

    /** Partitions update counters. */
    @GridToStringInclude
    private Map<Integer, CachePartitionPartialCountersMap> partCntrs;

    /** Partitions sizes. */
    @GridToStringInclude
    private Map<Integer, Map<Integer, Long>> partsSizes;

    /** Partitions history reservation counters. */
    @GridToStringInclude
    private Map<Integer, Map<Integer, Long>> partHistCntrs;

    /** Exception. */
    @GridToStringInclude
    private Exception err;

    /** */
    @Order(6)
    private boolean client;

    /** */
    @Order(value = 7, method = "duplicatedPartitionsData")
    private Map<Integer, Integer> dupPartsData;

    /** */
    @Order(value = 8, method = "errorBytes")
    private byte[] errBytes;

    /** Start time of exchange on node which sent this message in nanoseconds. */
    @Order(9)
    private long exchangeStartTime;

    /**
     * Exchange finish message, sent to new coordinator when it tries to restore state after previous coordinator failed
     * during exchange.
     */
    @Order(value = 10, method = "finishMessage")
    private GridDhtPartitionsFullMessage finishMsg;

    /** */
    @Order(value = 11, method = "groupsAffinityRequest")
    private Collection<Integer> grpsAffReq;

    /** Serialized partitions counters. */
    @Order(value = 12, method = "partitionCounterBytes")
    private byte[] partCntrsBytes;

    /** Serialized partitions history reservation counters. */
    @Order(value = 13, method = "partitionHistoryCounterBytes")
    private byte[] partHistCntrsBytes;

    /** Serialized local partitions. Unmarshalled to {@link #parts}. */
    @Order(value = 14, method = "partitionBytes")
    private byte[] partsBytes;

    /** Serialized partitions counters. */
    @Order(value = 15, method = "partitionSizesBytes")
    private byte[] partsSizesBytes;

    /**
     * Empty constructor.
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

    /** Sets the duplicated data. */
    public void duplicatedPartitionsData(Map<Integer, Integer> dupPartsData) {
        this.dupPartsData = dupPartsData;
    }

    /** @return the duplicated data. */
    public Map<Integer, Integer> duplicatedPartitionsData() {
        return dupPartsData;
    }

    /**
     * @param finishMsg Exchange finish message (used to restore exchange state on new coordinator).
     */
    public void finishMessage(GridDhtPartitionsFullMessage finishMsg) {
        this.finishMsg = finishMsg;
    }

    /**
     * @return Exchange finish message (used to restore exchange state on new coordinator).
     */
    public GridDhtPartitionsFullMessage finishMessage() {
        return finishMsg;
    }

    /**
     * @param grpsAffRequest Cache groups to get affinity for (affinity is requested when node joins cluster).
     */
    public void cacheGroupsAffinityRequest(Collection<Integer> grpsAffRequest) {
        this.grpsAffReq = grpsAffRequest;
    }

    /**
     * @return Cache groups to get affinity for (affinity is requested when node joins cluster).
     */
    @Nullable public Collection<Integer> cacheGroupsAffinityRequest() {
        return grpsAffReq;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /**
     * Sets the client flag.
     */
    public void client(boolean client) {
        this.client = client;
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
    public void addPartitionUpdateCounters(int grpId, CachePartitionPartialCountersMap cntrMap) {
        if (partCntrs == null)
            partCntrs = new HashMap<>();

        partCntrs.put(grpId, cntrMap);
    }

    /** @return Partition update counters per cache group. */
    public Map<Integer, CachePartitionPartialCountersMap> partitionUpdateCounters() {
        return partCntrs == null ? Collections.emptyMap() : Collections.unmodifiableMap(partCntrs);
    }

    /**
     * @param grpId Cache group ID.
     * @return Partition update counters.
     */
    public CachePartitionPartialCountersMap partitionUpdateCounters(int grpId) {
        CachePartitionPartialCountersMap res = partCntrs == null ? null : partCntrs.get(grpId);

        return res == null ? CachePartitionPartialCountersMap.EMPTY : res;
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

    /** */
    public void groupsAffinityRequest(Collection<Integer> grpsAffReq) {
        this.grpsAffReq = grpsAffReq;
    }

    /** */
    public Collection<Integer> groupsAffinityRequest() {
        return grpsAffReq;
    }

    /**  */
    public void errorBytes(byte[] errBytes) {
        this.errBytes = errBytes;
    }

    /** */
    public byte[] errorBytes() {
        return errBytes;
    }

    /** Sets the serialized local partitions. */
    public void partitionBytes(byte[] partsBytes) {
        this.partsBytes = partsBytes;
    }

    /** @return Serialized local partitions. */
    public byte[] partitionBytes() {
        return partsBytes;
    }

    /** @return Serialized partitions counters. */
    public byte[] partitionCounterBytes() {
        return partCntrsBytes;
    }

    /** Sets the serialized partitions counters. */
    public void partitionCounterBytes(byte[] partCntrsBytes) {
        this.partCntrsBytes = partCntrsBytes;
    }

    /** @return Serializaed partitions history reservation counters. */
    public byte[] partitionHistoryCounterBytes() {
        return partHistCntrsBytes;
    }

    /** Sets the serialized partitions history reservation counters. */
    public void partitionHistoryCounterBytes(byte[] partHistCntrsBytes) {
        this.partHistCntrsBytes = partHistCntrsBytes;
    }

    /** Sets the partitions counters. */
    public void partitionSizesBytes(byte[] partsSizesBytes) {
        this.partsSizesBytes = partsSizesBytes;
    }

    /** @return partitions counters. */
    public byte[] partitionSizesBytes() {
        return partsSizesBytes;
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
    @Override public short directType() {
        return 47;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsSingleMessage.class, this, super.toString());
    }
}
