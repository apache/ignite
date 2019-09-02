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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
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
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Partition supply message.
 */
@IgniteCodeGeneratingFail
public class GridDhtPartitionSupplyMessage extends GridCacheGroupIdMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** An unique (per demander) rebalance id. */
    private long rebalanceId;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Partitions that have been fully sent. */
    @GridDirectMap(keyType = int.class, valueType = long.class)
    private Map<Integer, Long> last;

    /** Partitions which were not found. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> missed;

    /** Partitions for which we were able to get historical iterator. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> clean;

    /** Entries. */
    @GridDirectMap(keyType = int.class, valueType = CacheEntryInfoCollection.class)
    private Map<Integer, CacheEntryInfoCollection> infos;

    /** Message size. */
    private int msgSize;

    /** Estimated keys count. */
    private long estimatedKeysCnt = -1;

    /** Estimated keys count per cache in case the message is for shared group. */
    @GridDirectMap(keyType = int.class, valueType = long.class)
    private Map<Integer, Long> keysPerCache;

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
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionSupplyMessage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /**
     * @return Rebalance id.
     */
    long rebalanceId() {
        return rebalanceId;
    }

    /**
     * @return Topology version for which demand message is sent.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Flag to indicate last message for partition.
     */
    Map<Integer, Long> last() {
        return last == null ? Collections.<Integer, Long>emptyMap() : last;
    }

    /**
     * @param p Partition which was fully sent.
     */
    void last(int p, long cntr) {
        if (last == null)
            last = new HashMap<>();

        if (last.put(p, cntr) == null) {
            msgSize += 12;

            // If partition is empty, we need to add it.
            if (!infos().containsKey(p)) {
                CacheEntryInfoCollection infoCol = new CacheEntryInfoCollection();

                infoCol.init();

                infos().put(p, infoCol);
            }
        }
    }

    /**
     * @param p Partition to clean.
     */
    void clean(int p) {
        if (clean == null)
            clean = new HashSet<>();

        if (clean.add(p))
            msgSize += 4;
    }

    /**
     * @param p Partition to check.
     * @return Check result.
     */
    boolean isClean(int p) {
        return clean != null && clean.contains(p);
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
    Collection<Integer> missed() {
        return missed == null ? Collections.<Integer>emptySet() : missed;
    }

    /**
     * @return Entries.
     */
    Map<Integer, CacheEntryInfoCollection> infos() {
        if (infos == null)
            infos = new HashMap<>();

        return infos;
    }

    /**
     * @return Message size.
     */
    int messageSize() {
        return msgSize;
    }

    /**
     * @param p Partition.
     * @param historical {@code True} if partition rebalancing using WAL history.
     * @param info Entry to add.
     * @param ctx Cache shared context.
     * @param cacheObjCtx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    void addEntry0(int p, boolean historical, GridCacheEntryInfo info, GridCacheSharedContext ctx,
        CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        assert info != null;
        assert info.key() != null : info;
        assert info.value() != null || historical : info;

        // Need to call this method to initialize info properly.
        marshalInfo(info, ctx, cacheObjCtx);

        msgSize += info.marshalledSize(cacheObjCtx);

        CacheEntryInfoCollection infoCol = infos().get(p);

        if (infoCol == null) {
            msgSize += 4;

            infos().put(p, infoCol = new CacheEntryInfoCollection());

            infoCol.init();
        }

        infoCol.add(info);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        if (grp == null)
            return;

        for (CacheEntryInfoCollection col : infos().values()) {
            List<GridCacheEntryInfo> entries = col.infos();

            for (int i = 0; i < entries.size(); i++)
                entries.get(i).unmarshal(grp.cacheObjectContext(), ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Number of entries in message.
     */
    public int size() {
        return infos().size();
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
            case 4:
                if (!writer.writeCollection("clean", clean, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("estimatedKeysCnt", estimatedKeysCnt))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap("infos", infos, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap("keysPerCache", keysPerCache, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap("last", last, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("missed", missed, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeInt("msgSize", msgSize))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 12:
                // Keep 'updateSeq' name for compatibility.
                if (!writer.writeLong("updateSeq", rebalanceId))
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
            case 4:
                clean = reader.readCollection("clean", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                estimatedKeysCnt = reader.readLong("estimatedKeysCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                infos = reader.readMap("infos", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                keysPerCache = reader.readMap("keysPerCache", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                last = reader.readMap("last", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                missed = reader.readCollection("missed", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msgSize = reader.readInt("msgSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                // Keep 'updateSeq' name for compatibility.
                rebalanceId = reader.readLong("updateSeq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionSupplyMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 114;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 13;
    }

    /**
     * @return Estimated keys count.
     */
    public long estimatedKeysCount() {
        return -1;
    }

    /**
     * @param cnt Keys count to add.
     */
    public void addEstimatedKeysCount(long cnt) {
        this.estimatedKeysCnt += cnt;
    }

    /**
     * @return Estimated keys count for a given cache ID.
     */
    public long keysForCache(int cacheId) {
        return -1;
    }

    /**
     * @param cacheId Cache ID.
     * @param cnt Keys count.
     */
    public void addKeysForCache(int cacheId, long cnt) {
        assert cacheId != 0 && cnt >= 0;

        if (keysPerCache == null)
            keysPerCache = new HashMap<>();

        Long cnt0 = keysPerCache.get(cacheId);

        if (cnt0 == null) {
            keysPerCache.put(cacheId, cnt);

            msgSize += 12;
        }
        else
            keysPerCache.put(cacheId, cnt0 + cnt);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionSupplyMessage.class, this,
            "size", size(),
            "parts", infos().keySet(),
            "super", super.toString());
    }
}
