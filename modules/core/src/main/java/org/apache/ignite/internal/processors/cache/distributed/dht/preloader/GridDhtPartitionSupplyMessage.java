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
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Partition supply message.
 */
public class GridDhtPartitionSupplyMessage extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Worker ID. */
    private int workerId = -1;

    /** Update sequence. */
    private long updateSeq;

    /** Acknowledgement flag. */
    private boolean ack;

    /** Partitions that have been fully sent. */
    @GridDirectCollection(int.class)
    private Collection<Integer> last;

    /** Partitions which were not found. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> missed;

    /** Entries. */
    @GridDirectMap(keyType = int.class, valueType = CacheEntryInfoCollection.class)
    private Map<Integer, CacheEntryInfoCollection> infos = new HashMap<>();

    /** Message size. */
    @GridDirectTransient
    private int msgSize;

    /**
     * @param workerId Worker ID.
     * @param updateSeq Update sequence for this node.
     * @param cacheId Cache ID.
     */
    GridDhtPartitionSupplyMessage(int workerId, long updateSeq, int cacheId) {
        assert workerId >= 0;
        assert updateSeq > 0;

        this.cacheId = cacheId;
        this.updateSeq = updateSeq;
        this.workerId = workerId;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionSupplyMessage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /**
     * @return Worker ID.
     */
    int workerId() {
        return workerId;
    }

    /**
     * @return Update sequence.
     */
    long updateSequence() {
        return updateSeq;
    }

    /**
     * Marks this message for acknowledgment.
     */
    void markAck() {
        ack = true;
    }

    /**
     * @return Acknowledgement flag.
     */
    boolean ack() {
        return ack;
    }

    /**
     * @return Flag to indicate last message for partition.
     */
    Collection<Integer> last() {
        return last == null ? Collections.<Integer>emptySet() : last;
    }

    /**
     * @param p Partition which was fully sent.
     */
    void last(int p) {
        if (last == null)
            last = new HashSet<>();

        if (last.add(p)) {
            msgSize += 4;

            // If partition is empty, we need to add it.
            if (!infos.containsKey(p)) {
                CacheEntryInfoCollection infoCol = new CacheEntryInfoCollection();

                infoCol.init();

                infos.put(p, infoCol);
            }
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
    Collection<Integer> missed() {
        return missed == null ? Collections.<Integer>emptySet() : missed;
    }

    /**
     * @return Entries.
     */
    Map<Integer, CacheEntryInfoCollection> infos() {
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
     * @param info Entry to add.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    void addEntry(int p, GridCacheEntryInfo info, GridCacheContext ctx) throws IgniteCheckedException {
        assert info != null;

        marshalInfo(info, ctx);

        msgSize += info.marshalledSize(ctx);

        CacheEntryInfoCollection infoCol = infos.get(p);

        if (infoCol == null) {
            msgSize += 4;

            infos.put(p, infoCol = new CacheEntryInfoCollection());

            infoCol.init();
        }

        infoCol.add(info);
    }

    /**
     * @param p Partition.
     * @param info Entry to add.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    void addEntry0(int p, GridCacheEntryInfo info, GridCacheContext ctx) throws IgniteCheckedException {
        assert info != null;
        assert (info.key() != null || info.keyBytes() != null);
        assert info.value() != null;

        // Need to call this method to initialize info properly.
        marshalInfo(info, ctx);

        msgSize += info.marshalledSize(ctx);

        CacheEntryInfoCollection infoCol = infos.get(p);

        if (infoCol == null) {
            msgSize += 4;

            infos.put(p, infoCol = new CacheEntryInfoCollection());

            infoCol.init();
        }

        infoCol.add(info);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cacheCtx = ctx.cacheContext(cacheId);

        for (CacheEntryInfoCollection col : infos().values()) {
            List<GridCacheEntryInfo>  entries = col.infos();

            for (int i = 0; i < entries.size(); i++)
                entries.get(i).unmarshal(cacheCtx, ldr);
        }
    }

    /**
     * @return Number of entries in message.
     */
    public int size() {
        return infos.size();
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
            case 3:
                if (!writer.writeBoolean("ack", ack))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap("infos", infos, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("last", last, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("missed", missed, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("updateSeq", updateSeq))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("workerId", workerId))
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
            case 3:
                ack = reader.readBoolean("ack");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                infos = reader.readMap("infos", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                last = reader.readCollection("last", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                missed = reader.readCollection("missed", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                updateSeq = reader.readLong("updateSeq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                workerId = reader.readInt("workerId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionSupplyMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 45;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 9;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionSupplyMessage.class, this,
            "size", size(),
            "parts", infos.keySet(),
            "super", super.toString());
    }
}