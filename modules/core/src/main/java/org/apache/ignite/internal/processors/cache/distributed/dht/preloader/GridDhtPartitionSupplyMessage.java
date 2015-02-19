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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Partition supply message.
 */
public class GridDhtPartitionSupplyMessage<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
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
    @GridDirectTransient
    private Map<Integer, Collection<GridCacheEntryInfo<K, V>>> infos = new HashMap<>();

    /** Cache entries in serialized form. */
    @GridToStringExclude
    @GridDirectTransient
    private Map<Integer, Collection<byte[]>> infoBytesMap = new HashMap<>();

    /** */
    private byte[] infoBytes;

    /** Message size. */
    @GridDirectTransient
    private int msgSize;

    /**
     * @param workerId Worker ID.
     * @param updateSeq Update sequence for this node.
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
            Collection<byte[]> serInfo = infoBytesMap.get(p);

            if (serInfo == null)
                infoBytesMap.put(p, new LinkedList<byte[]>());
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
    Map<Integer, Collection<GridCacheEntryInfo<K, V>>> infos() {
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
    void addEntry(int p, GridCacheEntryInfo<K, V> info, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert info != null;

        marshalInfo(info, ctx);

        byte[] bytes = CU.marshal(ctx, info);

        msgSize += bytes.length;

        Collection<byte[]> serInfo = infoBytesMap.get(p);

        if (serInfo == null) {
            msgSize += 4;

            infoBytesMap.put(p, serInfo = new LinkedList<>());
        }

        serInfo.add(bytes);
    }

    /**
     * @param p Partition.
     * @param info Entry to add.
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    void addEntry0(int p, GridCacheEntryInfo<K, V> info, GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        assert info != null;
        assert info.keyBytes() != null;
        assert info.valueBytes() != null || info.value() instanceof byte[] :
            "Missing value bytes with invalid value: " + info.value();

        // Need to call this method to initialize info properly.
        marshalInfo(info, ctx);

        byte[] bytes = CU.marshal(ctx, info);

        msgSize += bytes.length;

        Collection<byte[]> serInfo = infoBytesMap.get(p);

        if (serInfo == null) {
            msgSize += 4;

            infoBytesMap.put(p, serInfo = new LinkedList<>());
        }

        serInfo.add(bytes);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        infoBytes = ctx.marshaller().marshal(infoBytesMap);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        infoBytesMap = ctx.marshaller().unmarshal(infoBytes, ldr);

        GridCacheContext<K, V> cacheCtx = ctx.cacheContext(cacheId);

        for (Map.Entry<Integer, Collection<byte[]>> e : infoBytesMap.entrySet()) {
            Collection<GridCacheEntryInfo<K, V>> entries = unmarshalCollection(e.getValue(), ctx, ldr);

            unmarshalInfos(entries, cacheCtx, ldr);

            infos.put(e.getKey(), entries);
        }
    }

    /**
     * @return Number of entries in message.
     */
    public int size() {
        return infos.isEmpty() ? infoBytesMap.size() : infos.size();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), (byte)9))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeBoolean("ack", ack))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("infoBytes", infoBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("last", last, Type.INT))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("missed", missed, Type.INT))
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
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                ack = reader.readBoolean("ack");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                infoBytes = reader.readByteArray("infoBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                last = reader.readCollection("last", Type.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                missed = reader.readCollection("missed", Type.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                updateSeq = reader.readLong("updateSeq");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                workerId = reader.readInt("workerId");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 45;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionSupplyMessage.class, this,
            "size", size(),
            "parts", infos.keySet(),
            "super", super.toString());
    }
}
