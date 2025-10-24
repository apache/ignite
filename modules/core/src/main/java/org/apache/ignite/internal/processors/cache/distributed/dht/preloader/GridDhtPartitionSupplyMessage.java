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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Partition supply message.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridDhtPartitionSupplyMessage extends GridCacheGroupIdMessage implements GridCacheDeployable {
    /** An unique (per demander) rebalance id. */
    @Order(4)
    private long rebalanceId;

    /** Topology version for which demand message is sent. */
    @Order(value = 5, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Partitions that have been fully sent. */
    @Order(6)
    private Map<Integer, Long> last;

    /** Partitions which were not found. */
    @GridToStringInclude
    @Order(7)
    private Collection<Integer> missed;

    /** Entries. */
    @Order(8)
    private Map<Integer, CacheEntryInfoCollection> infos;

    /** Message size. */
    @Order(value = 9, method = "messageSize")
    private int msgSize;

    /** Supplying process error. */
    private Throwable err;

    // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-26523
    /** Serialized form of supplying process error. */
    @Order(10)
    private byte[] errBytes;

    /**
     * @param rebalanceId Rebalance id.
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     */
    GridDhtPartitionSupplyMessage(
        long rebalanceId,
        int grpId,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        this.grpId = grpId;
        this.rebalanceId = rebalanceId;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param rebalanceId Rebalance id.
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     */
    GridDhtPartitionSupplyMessage(
        long rebalanceId,
        int grpId,
        AffinityTopologyVersion topVer,
        boolean addDepInfo,
        Throwable err
    ) {
        this.grpId = grpId;
        this.rebalanceId = rebalanceId;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
        this.err = err;
    }

    /**
     * Empty constructor.
     */
    public GridDhtPartitionSupplyMessage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /**
     * @return An unique (per demander) rebalance id.
     */
    public long rebalanceId() {
        return rebalanceId;
    }

    /**
     * @param rebalanceId New unique (per demander) rebalance id.
     */
    public void rebalanceId(long rebalanceId) {
        this.rebalanceId = rebalanceId;
    }

    /**
     * @return Topology version for which demand message is sent.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version for which demand message is sent.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Partitions that have been fully sent.
     */
    public Map<Integer, Long> last() {
        return last;
    }

    /**
     * @param last New map of partitions that have been fully sent.
     */
    public void last(Map<Integer, Long> last) {
        this.last = last;
    }

    /**
     * @param p Partition which was fully sent.
     */
    void addLast(int p, long cntr) {
        if (last == null)
            last = new HashMap<>();

        if (last.put(p, cntr) == null) {
            msgSize += 12;

            // If partition is empty, we need to add it.
            if (!getInfosSafe().containsKey(p))
                getInfosSafe().put(p, new CacheEntryInfoCollection());
        }
    }

    /**
     * @param p Missed partition.
     */
    void missed(int p) {
        if (missed == null)
            missed = new HashSet<>();

        if (missed.add(p))
            msgSize += 4;
    }

    /**
     * @return Missed partitions.
     */
    public Collection<Integer> missed() {
        return missed;
    }

    /**
     * @param missed New partitions which were not found.
     */
    public void missed(Collection<Integer> missed) {
        this.missed = missed;
    }

    /**
     * @return Entries.
     */
    public Map<Integer, CacheEntryInfoCollection> getInfosSafe() {
        if (infos == null)
            infos = new HashMap<>();

        return infos;
    }

    /**
     * @return Entries.
     */
    public Map<Integer, CacheEntryInfoCollection> infos() {
        return infos;
    }

    /**
     * @param infos New entries.
     */
    public void infos(Map<Integer, CacheEntryInfoCollection> infos) {
        this.infos = infos;
    }

    /** Supplying process error. */
    @Nullable @Override public Throwable error() {
        return err;
    }

    /**
     * @return Serialized form of supplying process error.
     */
    public byte[] errBytes() {
        return errBytes;
    }

    /**
     * @param errBytes New serialized form of supplying process error.
     */
    public void errBytes(byte[] errBytes) {
        this.errBytes = errBytes;
    }

    /**
     * @return Message size.
     */
    public int messageSize() {
        return msgSize;
    }

    /**
     * @param msgSize New message size.
     */
    public void messageSize(int msgSize) {
        this.msgSize = msgSize;
    }

    /**
     * @param p Partition.
     * @param historical {@code True} if partition rebalancing using WAL history.
     * @param info Entry to add.
     * @param ctx Cache shared context.
     * @param cacheObjCtx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    void addEntry0(int p, boolean historical, GridCacheEntryInfo info, GridCacheSharedContext<?, ?> ctx,
        CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        assert info != null;
        assert info.key() != null : info;
        assert info.value() != null || historical : info;

        // Need to call this method to initialize info properly.
        marshalInfo(info, ctx, cacheObjCtx);

        msgSize += info.marshalledSize(cacheObjCtx);

        CacheEntryInfoCollection infoCol = getInfosSafe().get(p);

        if (infoCol == null) {
            msgSize += 4;

            getInfosSafe().put(p, infoCol = new CacheEntryInfoCollection());
        }

        infoCol.add(info);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-26523
        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx, err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        if (grp == null)
            return;

        for (CacheEntryInfoCollection col : getInfosSafe().values()) {
            List<GridCacheEntryInfo> entries = col.infos();

            for (int i = 0; i < entries.size(); i++)
                entries.get(i).unmarshal(grp.cacheObjectContext(), ldr);
        }

        // TODO: Should be removed in https://issues.apache.org/jira/browse/IGNITE-26523
        if (errBytes != null && err == null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Number of entries in message.
     */
    public int size() {
        return getInfosSafe().size();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 114;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionSupplyMessage.class, this,
            "size", size(),
            "parts", getInfosSafe().keySet(),
            "super", super.toString());
    }
}
