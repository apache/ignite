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
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Preload supply message.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedPreloadSupplyMessage<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Last flag. */
    private boolean last;

    /** Worker ID. */
    private int workerId = -1;

    /** Failed flag. {@code True} if sender cannot supply. */
    private boolean failed;

    /** Cache preload entries in serialized form. */
    @GridToStringExclude
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> entryBytes = new LinkedList<>();

    /** Message size. */
    private int msgSize;

    /** Cache entries. */
    @GridToStringExclude
    @GridDirectTransient
    private List<GridCacheEntryInfo<K, V>> entries = new LinkedList<>();

    /**
     * @param workerId Worker ID.
     */
    GridReplicatedPreloadSupplyMessage(int workerId) {
        this.workerId = workerId;
    }

    /**
     * @param workerId Worker ID.
     * @param failed Failed flag.
     */
    GridReplicatedPreloadSupplyMessage(int workerId, boolean failed) {
        this.workerId = workerId;
        this.failed = failed;
    }

    /**
     * Required by {@link Externalizable}.
     */
    public GridReplicatedPreloadSupplyMessage() {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        // NOTE: we don't need to prepare entryBytes here since we do it
        // iteratively using method addSerializedEntry().
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        entries = unmarshalCollection(entryBytes, ctx, ldr);

        unmarshalInfos(entries, ctx, ldr);
    }

    /**
     * @param info Entry to add.
     * @param ctx Cache context.
     * @throws GridException If failed.
     */
    void addEntry(GridCacheEntryInfo<K, V> info, GridCacheContext<K, V> ctx) throws GridException {
        assert info != null;

        marshalInfo(info, ctx);

        byte[] bytes = CU.marshal(ctx, info);

        msgSize += bytes.length;

        entryBytes.add(bytes);
    }

    /**
     * @param info Entry to add.
     * @param ctx Cache context.
     * @throws GridException If failed.
     */
    void addEntry0(GridCacheEntryInfo<K, V> info, GridCacheContext<K, V> ctx) throws GridException {
        assert info != null;
        assert info.keyBytes() != null;
        assert info.valueBytes() != null;

        // Need to call this method to initialize info properly.
        marshalInfo(info, ctx);

        byte[] bytes = CU.marshal(ctx, info);

        msgSize += bytes.length;

        entryBytes.add(bytes);
    }

    /**
     * @return {@code true} if this is the last batch..
     */
    boolean last() {
        return last;
    }

    /**
     * @param last {@code true} if this is the last batch.
     */
    void last(boolean last) {
        this.last = last;
    }

    /**
     * @return Worker ID.
     */
    int workerId() {
        return workerId;
    }

    /**
     * @return {@code True} if preloading from this node failed.
     */
    boolean failed() {
        return failed;
    }

    /**
     * @return Entries.
     */
     Collection<GridCacheEntryInfo<K, V>> entries() {
        return entries;
    }

    /**
     * @return Message size in bytes.
     */
    int size() {
        return msgSize;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridReplicatedPreloadSupplyMessage _clone = new GridReplicatedPreloadSupplyMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridReplicatedPreloadSupplyMessage _clone = (GridReplicatedPreloadSupplyMessage)_msg;

        _clone.last = last;
        _clone.workerId = workerId;
        _clone.failed = failed;
        _clone.entryBytes = entryBytes;
        _clone.msgSize = msgSize;
        _clone.entries = entries;
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
                if (entryBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(entryBytes.size()))
                            return false;

                        commState.it = entryBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 3:
                if (!commState.putBoolean(failed))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putBoolean(last))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putInt(msgSize))
                    return false;

                commState.idx++;

            case 6:
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
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (entryBytes == null)
                        entryBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        entryBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 3:
                if (buf.remaining() < 1)
                    return false;

                failed = commState.getBoolean();

                commState.idx++;

            case 4:
                if (buf.remaining() < 1)
                    return false;

                last = commState.getBoolean();

                commState.idx++;

            case 5:
                if (buf.remaining() < 4)
                    return false;

                msgSize = commState.getInt();

                commState.idx++;

            case 6:
                if (buf.remaining() < 4)
                    return false;

                workerId = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 60;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedPreloadSupplyMessage.class, this, "size", size());
    }
}
