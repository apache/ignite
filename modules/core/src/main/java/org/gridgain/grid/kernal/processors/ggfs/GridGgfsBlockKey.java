/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * File's binary data block key.
 */
@GridInternal
public final class GridGgfsBlockKey extends GridTcpCommunicationMessageAdapter
    implements Externalizable, Comparable<GridGgfsBlockKey> {
    /** */
    private static final long serialVersionUID = 0L;

    /** File system file ID. */
    private IgniteUuid fileId;

    /** Block ID. */
    private long blockId;

    /** Block affinity key. */
    private IgniteUuid affKey;

    /** Eviction exclude flag. */
    private boolean evictExclude;

    /**
     * Constructs file's binary data block key.
     *
     * @param fileId File ID.
     * @param affKey Affinity key.
     * @param evictExclude Evict exclude flag.
     * @param blockId Block ID.
     */
    public GridGgfsBlockKey(IgniteUuid fileId, @Nullable IgniteUuid affKey, boolean evictExclude, long blockId) {
        assert fileId != null;
        assert blockId >= 0;

        this.fileId = fileId;
        this.affKey = affKey;
        this.evictExclude = evictExclude;
        this.blockId = blockId;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridGgfsBlockKey() {
        // No-op.
    }

    /**
     * @return File ID.
     */
    public IgniteUuid getFileId() {
        return fileId;
    }

    /**
     * @return Block affinity key.
     */
    public IgniteUuid affinityKey() {
        return affKey;
    }

    /**
     * @return Evict exclude flag.
     */
    public boolean evictExclude() {
        return evictExclude;
    }

    /**
     * @return Block ID.
     */
    public long getBlockId() {
        return blockId;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridGgfsBlockKey o) {
        int res = fileId.compareTo(o.fileId);

        if (res != 0)
            return res;

        long v1 = blockId;
        long v2 = o.blockId;

        if (v1 != v2)
            return v1 > v2 ? 1 : -1;

        if (affKey == null && o.affKey == null)
            return 0;

        if (affKey != null && o.affKey != null)
            return affKey.compareTo(o.affKey);

        return affKey != null ? -1 : 1;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, fileId);
        U.writeGridUuid(out, affKey);
        out.writeBoolean(evictExclude);
        out.writeLong(blockId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        fileId = U.readGridUuid(in);
        affKey = U.readGridUuid(in);
        evictExclude = in.readBoolean();
        blockId = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return fileId.hashCode() + (int)(blockId ^ (blockId >>> 32));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || o.getClass() != getClass())
            return false;

        GridGgfsBlockKey that = (GridGgfsBlockKey)o;

        return blockId == that.blockId && fileId.equals(that.fileId) && F.eq(affKey, that.affKey) &&
            evictExclude == that.evictExclude;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsBlockKey _clone = new GridGgfsBlockKey();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridGgfsBlockKey _clone = (GridGgfsBlockKey)_msg;

        _clone.fileId = fileId;
        _clone.blockId = blockId;
        _clone.affKey = affKey;
        _clone.evictExclude = evictExclude;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putGridUuid(affKey))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putLong(blockId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putBoolean(evictExclude))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putGridUuid(fileId))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                IgniteUuid affKey0 = commState.getGridUuid();

                if (affKey0 == GRID_UUID_NOT_READ)
                    return false;

                affKey = affKey0;

                commState.idx++;

            case 1:
                if (buf.remaining() < 8)
                    return false;

                blockId = commState.getLong();

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                evictExclude = commState.getBoolean();

                commState.idx++;

            case 3:
                IgniteUuid fileId0 = commState.getGridUuid();

                if (fileId0 == GRID_UUID_NOT_READ)
                    return false;

                fileId = fileId0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 66;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsBlockKey.class, this);
    }
}
