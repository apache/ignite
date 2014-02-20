// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Initial request for preload data from a remote node.
 * Once the remote node has received this request it starts sending
 * cache entries split into batches.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedPreloadDemandMessage<K, V> extends GridCacheMessage<K, V> {
    /** Partition. */
    private int part;

    /** Mod. */
    private int mod;

    /** Node count. */
    private int cnt;

    /** Topic. */
    @GridDirectTransient
    private Object topic;

    /** Topic bytes. */
    private byte[] topicBytes;

    /** Timeout. */
    private long timeout;

    /** Worker ID. */
    private int workerId = -1;

    /**
     * Required by {@link Externalizable}.
     */
    public GridReplicatedPreloadDemandMessage() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param mod Mod to use.
     * @param cnt Size to use.
     * @param topic Topic.
     * @param timeout Timeout.
     * @param workerId Worker ID.
     */
    GridReplicatedPreloadDemandMessage(int part, int mod, int cnt, Object topic, long timeout, int workerId) {
        this.part = part;
        this.mod = mod;
        this.cnt = cnt;
        this.topic = topic;
        this.timeout = timeout;
        this.workerId = workerId;
    }

    /**
     * @param msg Message to copy from.
     */
    GridReplicatedPreloadDemandMessage(GridReplicatedPreloadDemandMessage msg) {
        part = msg.partition();
        mod = msg.mod();
        cnt = msg.nodeCount();
        topic = msg.topic();
        timeout = msg.timeout();
        workerId = msg.workerId();
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Partition.
     */
    int partition() {
        return part;
    }

    /**
     * @return Mod to use for key selection.
     */
    int mod() {
        return mod;
    }

    /**
     *
     * @return Number to use as a denominator when calculating a mod.
     */
    int nodeCount() {
        return cnt;
    }

    /**
     * @return Topic.
     */
    public Object topic() {
        return topic;
    }

    /**
     * @param topic Topic.
     */
    public void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Timeout.
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

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (topic != null)
            topicBytes = ctx.marshaller().marshal(topic);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (topicBytes != null)
            topic = ctx.marshaller().unmarshal(topicBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridReplicatedPreloadDemandMessage _clone = new GridReplicatedPreloadDemandMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridReplicatedPreloadDemandMessage _clone = (GridReplicatedPreloadDemandMessage)_msg;

        _clone.part = part;
        _clone.mod = mod;
        _clone.cnt = cnt;
        _clone.topic = topic;
        _clone.topicBytes = topicBytes;
        _clone.timeout = timeout;
        _clone.workerId = workerId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 2:
                if (!commState.putInt(cnt))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putInt(mod))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putInt(part))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putLong(timeout))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putByteArray(topicBytes))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putInt(workerId))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 2:
                if (buf.remaining() < 4)
                    return false;

                cnt = commState.getInt();

                commState.idx++;

            case 3:
                if (buf.remaining() < 4)
                    return false;

                mod = commState.getInt();

                commState.idx++;

            case 4:
                if (buf.remaining() < 4)
                    return false;

                part = commState.getInt();

                commState.idx++;

            case 5:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong();

                commState.idx++;

            case 6:
                byte[] topicBytes0 = commState.getByteArray();

                if (topicBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                topicBytes = topicBytes0;

                commState.idx++;

            case 7:
                if (buf.remaining() < 4)
                    return false;

                workerId = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 55;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedPreloadDemandMessage.class, this);
    }
}
