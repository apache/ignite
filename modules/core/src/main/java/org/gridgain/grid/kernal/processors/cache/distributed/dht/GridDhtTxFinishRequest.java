/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near transaction finish request.
 */
public class GridDhtTxFinishRequest<K, V> extends GridDistributedTxFinishRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Transaction isolation. */
    private IgniteTxIsolation isolation;

    /** Near writes. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxEntry<K, V>> nearWrites;

    /** Serialized near writes. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> nearWritesBytes;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** System invalidation flag. */
    private boolean sysInvalidate;

    /** Topology version. */
    private long topVer;

    /** Pending versions with order less than one for this message (needed for commit ordering). */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pendingVers;

    /** One phase commit flag for fast-commit path. */
    private boolean onePhaseCommit;

    /** One phase commit write version. */
    private GridCacheVersion writeVer;

    /** Subject ID. */
    @GridDirectVersion(1)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(2)
    private int taskNameHash;

    /** TTLs for optimistic transaction. */
    private GridLongList ttls;

    /** Near cache TTLs for optimistic transaction. */
    private GridLongList nearTtls;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxFinishRequest() {
        // No-op.
    }

    /**
     * @param nearNodeId Near node ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param topVer Topology version.
     * @param xidVer Transaction ID.
     * @param threadId Thread ID.
     * @param commitVer Commit version.
     * @param isolation Transaction isolation.
     * @param commit Commit flag.
     * @param invalidate Invalidate flag.
     * @param sys System flag.
     * @param sysInvalidate System invalidation flag.
     * @param syncCommit Synchronous commit flag.
     * @param syncRollback Synchronous rollback flag.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pendingVers Pending versions.
     * @param txSize Expected transaction size.
     * @param writes Write entries.
     * @param nearWrites Near cache writes.
     * @param recoverWrites Recovery write entries.
     * @param onePhaseCommit One phase commit flag.
     * @param grpLockKey Group lock key.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     */
    public GridDhtTxFinishRequest(
        UUID nearNodeId,
        IgniteUuid futId,
        IgniteUuid miniId,
        long topVer,
        GridCacheVersion xidVer,
        GridCacheVersion commitVer,
        long threadId,
        IgniteTxIsolation isolation,
        boolean commit,
        boolean invalidate,
        boolean sys,
        boolean sysInvalidate,
        boolean syncCommit,
        boolean syncRollback,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pendingVers,
        int txSize,
        Collection<IgniteTxEntry<K, V>> writes,
        Collection<IgniteTxEntry<K, V>> nearWrites,
        Collection<IgniteTxEntry<K, V>> recoverWrites,
        boolean onePhaseCommit,
        @Nullable IgniteTxKey grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(xidVer, futId, commitVer, threadId, commit, invalidate, sys, syncCommit, syncRollback, baseVer,
            committedVers, rolledbackVers, txSize, writes, recoverWrites, grpLockKey);

        assert miniId != null;
        assert nearNodeId != null;
        assert isolation != null;

        this.pendingVers = pendingVers;
        this.topVer = topVer;
        this.nearNodeId = nearNodeId;
        this.isolation = isolation;
        this.nearWrites = nearWrites;
        this.miniId = miniId;
        this.sysInvalidate = sysInvalidate;
        this.onePhaseCommit = onePhaseCommit;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Near writes.
     */
    public Collection<IgniteTxEntry<K, V>> nearWrites() {
        return nearWrites == null ? Collections.<IgniteTxEntry<K, V>>emptyList() : nearWrites;
    }

    /**
     * @return Mini ID.
     */
    public IgniteUuid miniId() {
        return miniId;
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
     * @return Transaction isolation.
     */
    public IgniteTxIsolation isolation() {
        return isolation;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return System invalidate flag.
     */
    public boolean isSystemInvalidate() {
        return sysInvalidate;
    }

    /**
     * @return One phase commit flag.
     */
    public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /**
     * @return Write version for one-phase commit transactions.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @param writeVer Write version for one-phase commit transactions.
     */
    public void writeVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * Gets versions of not acquired locks with version less then one of transaction being committed.
     *
     * @return Versions of locks for entries participating in transaction that have not been acquired yet
     *      have version less then one of transaction being committed.
     */
    public Collection<GridCacheVersion> pendingVersions() {
        return pendingVers == null ? Collections.<GridCacheVersion>emptyList() : pendingVers;
    }

    /**
     * @param idx Entry index.
     * @param ttl TTL.
     */
    public void ttl(int idx, long ttl) {
        if (ttl != -1L) {
            if (ttls == null) {
                ttls = new GridLongList();

                for (int i = 0; i < idx - 1; i++)
                    ttls.add(-1L);
            }
        }

        if (ttls != null)
            ttls.add(ttl);
    }

    /**
     * @return TTLs for optimistic transaction.
     */
    public GridLongList ttls() {
        return ttls;
    }

    /**
     * @param idx Entry index.
     * @param ttl TTL.
     */
    public void nearTtl(int idx, long ttl) {
        if (ttl != -1L) {
            if (nearTtls == null) {
                nearTtls = new GridLongList();

                for (int i = 0; i < idx - 1; i++)
                    nearTtls.add(-1L);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);
    }

    /**
     * @return TTLs for optimistic transaction.
     */
    public GridLongList nearTtls() {
        return nearTtls;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearWrites != null) {
            marshalTx(nearWrites, ctx);

            nearWritesBytes = new ArrayList<>(nearWrites.size());

            for (IgniteTxEntry<K, V> e : nearWrites)
                nearWritesBytes.add(ctx.marshaller().marshal(e));
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (nearWritesBytes != null) {
            nearWrites = new ArrayList<>(nearWritesBytes.size());

            for (byte[] arr : nearWritesBytes)
                nearWrites.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(nearWrites, true, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxFinishRequest.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtTxFinishRequest _clone = new GridDhtTxFinishRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtTxFinishRequest _clone = (GridDhtTxFinishRequest)_msg;

        _clone.nearNodeId = nearNodeId;
        _clone.isolation = isolation;
        _clone.nearWrites = nearWrites;
        _clone.nearWritesBytes = nearWritesBytes;
        _clone.miniId = miniId;
        _clone.sysInvalidate = sysInvalidate;
        _clone.topVer = topVer;
        _clone.pendingVers = pendingVers;
        _clone.onePhaseCommit = onePhaseCommit;
        _clone.writeVer = writeVer;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
        _clone.ttls = ttls;
        _clone.nearTtls = nearTtls;
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
                if (!commState.putEnum(isolation))
                    return false;

                commState.idx++;

            case 22:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 23:
                if (!commState.putUuid(nearNodeId))
                    return false;

                commState.idx++;

            case 24:
                if (nearWritesBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(nearWritesBytes.size()))
                            return false;

                        commState.it = nearWritesBytes.iterator();
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

            case 25:
                if (!commState.putBoolean(onePhaseCommit))
                    return false;

                commState.idx++;

            case 26:
                if (pendingVers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(pendingVers.size()))
                            return false;

                        commState.it = pendingVers.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putCacheVersion((GridCacheVersion)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 27:
                if (!commState.putBoolean(sysInvalidate))
                    return false;

                commState.idx++;

            case 28:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 29:
                if (!commState.putCacheVersion(writeVer))
                    return false;

                commState.idx++;

            case 30:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

            case 31:
                if (!commState.putInt(taskNameHash))
                    return false;

                commState.idx++;

            case 32:
                if (!commState.putLongList(ttls))
                    return false;

                commState.idx++;

            case 33:
                if (!commState.putLongList(nearTtls))
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
                if (buf.remaining() < 1)
                    return false;

                byte isolation0 = commState.getByte();

                isolation = IgniteTxIsolation.fromOrdinal(isolation0);

                commState.idx++;

            case 22:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 23:
                UUID nearNodeId0 = commState.getUuid();

                if (nearNodeId0 == UUID_NOT_READ)
                    return false;

                nearNodeId = nearNodeId0;

                commState.idx++;

            case 24:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (nearWritesBytes == null)
                        nearWritesBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        nearWritesBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 25:
                if (buf.remaining() < 1)
                    return false;

                onePhaseCommit = commState.getBoolean();

                commState.idx++;

            case 26:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (pendingVers == null)
                        pendingVers = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        pendingVers.add((GridCacheVersion)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 27:
                if (buf.remaining() < 1)
                    return false;

                sysInvalidate = commState.getBoolean();

                commState.idx++;

            case 28:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 29:
                GridCacheVersion writeVer0 = commState.getCacheVersion();

                if (writeVer0 == CACHE_VER_NOT_READ)
                    return false;

                writeVer = writeVer0;

                commState.idx++;

            case 30:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 31:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt();

                commState.idx++;

            case 32:
                GridLongList ttls0 = commState.getLongList();

                if (ttls0 == LONG_LIST_NOT_READ)
                    return false;

                ttls = ttls0;

                commState.idx++;

            case 33:
                GridLongList nearTtls0 = commState.getLongList();

                if (nearTtls0 == LONG_LIST_NOT_READ)
                    return false;

                nearTtls = nearTtls0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 31;
    }
}
