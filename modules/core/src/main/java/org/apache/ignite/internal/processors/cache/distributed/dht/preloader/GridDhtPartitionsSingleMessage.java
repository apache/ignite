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
import org.apache.ignite.internal.managers.communication.ErrorMessage;
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

    /**
     * Serialized local partitions. Unmarshalled to {@link #parts}.
     * <p>
     * TODO Remove this field after completing task IGNITE-26976.
     */
    @Order(6)
    byte[] partsBytes;

    /** */
    @Order(7)
    Map<Integer, Integer> dupPartsData;

    /** Partitions update counters. */
    @Order(8)
    @GridToStringInclude
    Map<Integer, CachePartitionPartialCountersMap> partCntrs;

    /** Partitions sizes. */
    @Order(9)
    @GridToStringInclude
    Map<Integer, IntLongMap> partsSizes;

    /** Partitions history reservation counters. */
    @Order(10)
    @GridToStringInclude
    Map<Integer, IntLongMap> partHistCntrs;

    /** Error message. */
    @Order(11)
    @GridToStringInclude
    ErrorMessage errMsg;

    /** */
    @Order(12)
    boolean client;

    /** */
    @Order(13)
    Collection<Integer> grpsAffReq;

    /** Start time of exchange on node which sent this message in nanoseconds. */
    @Order(14)
    long exchangeStartTime;

    /**
     * Exchange finish message, sent to new coordinator when it tries to restore state after previous coordinator failed
     * during exchange.
     */
    @Order(15)
    GridDhtPartitionsFullMessage finishMsg;

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
     * @param grpsAffReq Cache groups to get affinity for (affinity is requested when node joins cluster).
     */
    public void cacheGroupsAffinityRequest(Collection<Integer> grpsAffReq) {
        this.grpsAffReq = grpsAffReq;
    }

    /**
     * @return Cache groups to get affinity for (affinity is requested when node joins cluster).
     */
    @Nullable public Collection<Integer> cacheGroupsAffinityRequest() {
        return grpsAffReq;
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
        return partCntrs;
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

        partsSizes.put(grpId, new IntLongMap(partSizesMap));
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

        IntLongMap sizesMap = partsSizes.get(grpId);

        return sizesMap != null ? F.emptyIfNull(sizesMap.map()) : Collections.emptyMap();
    }

    /**
     * @param cntrMap Partition history counters.
     */
    void partitionHistoryCounters(Map<Integer, Map<Integer, Long>> cntrMap) {
        for (Map.Entry<Integer, Map<Integer, Long>> e : cntrMap.entrySet()) {
            Map<Integer, Long> historyCntrs = e.getValue();

            if (historyCntrs.isEmpty())
                continue;

            if (partHistCntrs == null)
                partHistCntrs = new HashMap<>();

            partHistCntrs.put(e.getKey(), new IntLongMap(historyCntrs));
        }
    }

    /**
     * @param grpId Cache group ID.
     * @return Partition history counters.
     */
    Map<Integer, Long> partitionHistoryCounters(int grpId) {
        if (partHistCntrs != null) {
            IntLongMap res = partHistCntrs.get(grpId);

            return res != null ? F.emptyIfNull(res.map()) : Collections.emptyMap();
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
    public void setError(Throwable ex) {
        errMsg = new ErrorMessage(ex);
    }

    /**
     * @return Not null exception if exchange processing failed.
     */
    @Nullable public Throwable getError() {
        return ErrorMessage.error(errMsg);
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
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (parts != null && partsBytes == null) {
            byte[] partsBytes0 = U.marshal(ctx, parts);

            if (compressed()) {
                try {
                    partsBytes0 = U.zip(partsBytes0);
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.logger(getClass()), "Failed to compress partitions data: " + e, e);
                }
            }

            partsBytes = partsBytes0;
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null && parts == null) {
            parts = compressed()
                ? U.unmarshalZip(ctx.marshaller(), partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()))
                : U.unmarshal(ctx, partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
