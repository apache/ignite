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
 * Partition demand request.
 */
public class GridDhtPartitionDemandMessage<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Update sequence. */
    private long updateSeq;

    /** Partition. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Set<Integer> parts;

    /** Topic. */
    @GridDirectTransient
    private Object topic;

    /** Serialized topic. */
    private byte[] topicBytes;

    /** Timeout. */
    private long timeout;

    /** Worker ID. */
    private int workerId = -1;

    /** Topology version. */
    private long topVer;

    /**
     * @param updateSeq Update sequence for this node.
     * @param topVer Topology version.
     */
    GridDhtPartitionDemandMessage(long updateSeq, long topVer, int cacheId) {
        assert updateSeq > 0;

        this.cacheId = cacheId;
        this.updateSeq = updateSeq;
        this.topVer = topVer;
    }

    /**
     * @param cp Message to copy from.
     */
    GridDhtPartitionDemandMessage(GridDhtPartitionDemandMessage<K, V> cp, Collection<Integer> parts) {
        cacheId = cp.cacheId;
        updateSeq = cp.updateSeq;
        topic = cp.topic;
        timeout = cp.timeout;
        workerId = cp.workerId;
        topVer = cp.topVer;

        // Create a copy of passed in collection since it can be modified when this message is being sent.
        this.parts = new HashSet<>(parts);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionDemandMessage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @param p Partition.
     */
    void addPartition(int p) {
        if (parts == null)
            parts = new HashSet<>();

        parts.add(p);
    }


    /**
     * @return Partition.
     */
    Set<Integer> partitions() {
        return parts;
    }

    /**
     * @return Update sequence.
     */
    long updateSequence() {
        return updateSeq;
    }

    /**
     * @return Reply message timeout.
     */
    long timeout() {
        return timeout;
    }

    /**
     * @param timeout Timeout.
     */
    void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Topic.
     */
    Object topic() {
        return topic;
    }

    /**
     * @param topic Topic.
     */
    void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Worker ID.
     */
    int workerId() {
        return workerId;
    }

    /**
     * @param workerId Worker ID.
     */
    void workerId(int workerId) {
        this.workerId = workerId;
    }

    /**
     * @return Topology version for which demand message is sent.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (topic != null)
            topicBytes = ctx.marshaller().marshal(topic);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (topicBytes != null)
            topic = ctx.marshaller().unmarshal(topicBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridDhtPartitionDemandMessage _clone = new GridDhtPartitionDemandMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtPartitionDemandMessage _clone = (GridDhtPartitionDemandMessage)_msg;

        _clone.updateSeq = updateSeq;
        _clone.parts = parts;
        _clone.topic = topic;
        _clone.topicBytes = topicBytes;
        _clone.timeout = timeout;
        _clone.workerId = workerId;
        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 3:
                if (!writer.writeCollection("parts", parts, int.class))
                    return false;

                state++;

            case 4:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                state++;

            case 5:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state++;

            case 6:
                if (!writer.writeByteArray("topicBytes", topicBytes))
                    return false;

                state++;

            case 7:
                if (!writer.writeLong("updateSeq", updateSeq))
                    return false;

                state++;

            case 8:
                if (!writer.writeInt("workerId", workerId))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 3:
                parts = reader.readCollection("parts", int.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 4:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 5:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 6:
                topicBytes = reader.readByteArray("topicBytes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 7:
                updateSeq = reader.readLong("updateSeq");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 8:
                workerId = reader.readInt("workerId");

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 44;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemandMessage.class, this, "partCnt", parts.size(), "super",
            super.toString());
    }
}
