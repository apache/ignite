/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near transaction finish request.
 */
public class GridNearTxFinishRequest<K, V> extends GridDistributedTxFinishRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Mini future ID. */
    private GridUuid miniId;

    /** Explicit lock flag. */
    private boolean explicitLock;

    /** Topology version. */
    private long topVer;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxFinishRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param xidVer Transaction ID.
     * @param threadId Thread ID.
     * @param commit Commit flag.
     * @param invalidate Invalidate flag.
     * @param explicitLock Explicit lock flag.
     * @param topVer Topology version.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param txSize Expected transaction size.
     * @param writeEntries Write entries.
     * @param reply Reply flag.
     * @param recoverEntries Recover entries.
     */
    public GridNearTxFinishRequest(
        GridUuid futId,
        GridCacheVersion xidVer,
        long threadId,
        boolean commit,
        boolean invalidate,
        boolean explicitLock,
        long topVer,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        int txSize,
        Collection<GridCacheTxEntry<K, V>> writeEntries,
        Collection<GridCacheTxEntry<K, V>> recoverEntries,
        boolean reply,
        @Nullable UUID subjId,
        int taskNameHash) {
        super(xidVer, futId, null, threadId, commit, invalidate, baseVer, committedVers, rolledbackVers, txSize,
            writeEntries, recoverEntries, reply, null);

        this.explicitLock = explicitLock;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return Explicit lock flag.
     */
    public boolean explicitLock() {
        return explicitLock;
    }

    /**
     * @return Mini future ID.
     */
    public GridUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(GridUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearTxFinishRequest _clone = new GridNearTxFinishRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearTxFinishRequest _clone = (GridNearTxFinishRequest)_msg;

        _clone.miniId = miniId;
        _clone.explicitLock = explicitLock;
        _clone.topVer = topVer;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
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
            case 19:
                if (!commState.putBoolean(explicitLock))
                    return false;

                commState.idx++;

            case 20:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 21:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 22:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

            case 23:
                if (!commState.putInt(taskNameHash))
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
            case 19:
                if (buf.remaining() < 1)
                    return false;

                explicitLock = commState.getBoolean();

                commState.idx++;

            case 20:
                GridUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 21:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 22:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 23:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 52;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridNearTxFinishRequest.class, this, "super", super.toString());
    }
}
