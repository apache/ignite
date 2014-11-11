/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

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
    private Set<Integer> last;

    /** Partitions which were not found. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Set<Integer> missed;

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

        this.updateSeq = updateSeq;
        this.workerId = workerId;

        cacheId(cacheId);
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
    Set<Integer> last() {
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
    Set<Integer> missed() {
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
     * @throws GridException If failed.
     */
    void addEntry(int p, GridCacheEntryInfo<K, V> info, GridCacheSharedContext<K, V> ctx) throws GridException {
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
     * @throws GridException If failed.
     */
    void addEntry0(int p, GridCacheEntryInfo<K, V> info, GridCacheSharedContext<K, V> ctx) throws GridException {
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
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        infoBytes = ctx.marshaller().marshal(infoBytesMap);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        infoBytesMap = ctx.marshaller().unmarshal(infoBytes, ldr);

        for (Map.Entry<Integer, Collection<byte[]>> e : infoBytesMap.entrySet()) {
            int cacheId = e.getKey();

            Collection<GridCacheEntryInfo<K, V>> entries = unmarshalCollection(e.getValue(), ctx, ldr);

            unmarshalInfos(entries, ctx.cacheContext(cacheId), ldr);

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
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtPartitionSupplyMessage _clone = new GridDhtPartitionSupplyMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtPartitionSupplyMessage _clone = (GridDhtPartitionSupplyMessage)_msg;

        _clone.workerId = workerId;
        _clone.updateSeq = updateSeq;
        _clone.ack = ack;
        _clone.last = last;
        _clone.missed = missed;
        _clone.infos = infos;
        _clone.infoBytesMap = infoBytesMap;
        _clone.infoBytes = infoBytes;
        _clone.msgSize = msgSize;
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
                if (!commState.putBoolean(ack))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putByteArray(infoBytes))
                    return false;

                commState.idx++;

            case 4:
                if (last != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(last.size()))
                            return false;

                        commState.it = last.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putInt((int)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 5:
                if (missed != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(missed.size()))
                            return false;

                        commState.it = missed.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putInt((int)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 6:
                if (!commState.putLong(updateSeq))
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
                if (buf.remaining() < 1)
                    return false;

                ack = commState.getBoolean();

                commState.idx++;

            case 3:
                byte[] infoBytes0 = commState.getByteArray();

                if (infoBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                infoBytes = infoBytes0;

                commState.idx++;

            case 4:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (last == null)
                        last = U.newHashSet(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (buf.remaining() < 4)
                            return false;

                        int _val = commState.getInt();

                        last.add((Integer)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 5:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (missed == null)
                        missed = U.newHashSet(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (buf.remaining() < 4)
                            return false;

                        int _val = commState.getInt();

                        missed.add((Integer)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 6:
                if (buf.remaining() < 8)
                    return false;

                updateSeq = commState.getLong();

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
        return 44;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionSupplyMessage.class, this,
            "size", size(),
            "parts", infos.keySet(),
            "super", super.toString());
    }
}
