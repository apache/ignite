/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near transaction prepare request.
 */
public class GridNearTxPrepareRequest<K, V> extends GridDistributedTxPrepareRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near mapping flag. */
    private boolean near;

    /** Topology version. */
    private long topVer;

    /** {@code True} if this last prepare request for node. */
    private boolean last;

    /** IDs of backup nodes receiving last prepare request during this prepare. */
    @GridDirectCollection(UUID.class)
    private Collection<UUID> lastBackups;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxPrepareRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param reads Read entries.
     * @param writes Write entries.
     * @param grpLockKey Group lock key if preparing group-lock transaction.
     * @param partLock {@code True} if preparing group-lock transaction with partition lock.
     * @param near {@code True} if mapping is for near caches.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this last prepare request for node.
     * @param lastBackups IDs of backup nodes receiving last prepare request during this prepare.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     */
    public GridNearTxPrepareRequest(
        IgniteUuid futId,
        long topVer,
        GridCacheTxEx<K, V> tx,
        Collection<GridCacheTxEntry<K, V>> reads,
        Collection<GridCacheTxEntry<K, V>> writes,
        GridCacheTxKey grpLockKey,
        boolean partLock,
        boolean near,
        Map<UUID, Collection<UUID>> txNodes,
        boolean last,
        Collection<UUID> lastBackups,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(tx, reads, writes, grpLockKey, partLock, txNodes);

        assert futId != null;

        this.futId = futId;
        this.topVer = topVer;
        this.near = near;
        this.last = last;
        this.lastBackups = lastBackups;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return IDs of backup nodes receiving last prepare request during this prepare.
     */
    public Collection<UUID> lastBackups() {
        return lastBackups;
    }

    /**
     * @return {@code True} if this last prepare request for node.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return {@code True} if mapping is for near-enabled caches.
     */
    public boolean near() {
        return near;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(IgniteUuid miniId) {
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

    /**
     *
     */
    public void cloneEntries() {
        reads(cloneEntries(reads()));
        writes(cloneEntries(writes()));
    }

    /**
     * Clones entries so that tx entries with initialized near entries are not passed to DHT transaction.
     * Used only when local part of prepare is invoked.
     *
     * @param c Collection of entries to clone.
     * @return Cloned collection.
     */
    private Collection<GridCacheTxEntry<K, V>> cloneEntries(Collection<GridCacheTxEntry<K, V>> c) {
        if (F.isEmpty(c))
            return c;

        Collection<GridCacheTxEntry<K, V>> cp = new ArrayList<>(c.size());

        for (GridCacheTxEntry<K, V> e : c) {
            GridCacheContext<K, V> cacheCtx = e.context();

            // Clone only if it is a near cache.
            if (cacheCtx.isNear())
                cp.add(e.cleanCopy(cacheCtx.nearTx().dht().context()));
            else
                cp.add(e);
        }

        return cp;
    }

    /** {@inheritDoc} */
    @Override protected boolean transferExpiryPolicy() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearTxPrepareRequest _clone = new GridNearTxPrepareRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearTxPrepareRequest _clone = (GridNearTxPrepareRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.topVer = topVer;
        _clone.last = last;
        _clone.lastBackups = lastBackups;
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
            case 21:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 22:
                if (!commState.putBoolean(last))
                    return false;

                commState.idx++;

            case 23:
                if (lastBackups != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(lastBackups.size()))
                            return false;

                        commState.it = lastBackups.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putUuid((UUID)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 24:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 25:
                if (!commState.putBoolean(near))
                    return false;

                commState.idx++;

            case 26:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 27:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

            case 28:
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
            case 21:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 22:
                if (buf.remaining() < 1)
                    return false;

                last = commState.getBoolean();

                commState.idx++;

            case 23:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (lastBackups == null)
                        lastBackups = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        UUID _val = commState.getUuid();

                        if (_val == UUID_NOT_READ)
                            return false;

                        lastBackups.add((UUID)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 24:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 25:
                if (buf.remaining() < 1)
                    return false;

                near = commState.getBoolean();

                commState.idx++;

            case 26:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 27:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 28:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 54;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareRequest.class, this, super.toString());
    }
}
