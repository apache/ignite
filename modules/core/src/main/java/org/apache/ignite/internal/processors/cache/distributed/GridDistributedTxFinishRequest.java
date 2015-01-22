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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Transaction completion message.
 */
public class GridDistributedTxFinishRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Thread ID. */
    private long threadId;

    /** Commit version. */
    private GridCacheVersion commitVer;

    /** Invalidate flag. */
    private boolean invalidate;

    /** Commit flag. */
    private boolean commit;

    /** Sync commit flag. */
    private boolean syncCommit;

    /** Sync commit flag. */
    private boolean syncRollback;

    /** Min version used as base for completed versions. */
    private GridCacheVersion baseVer;

    /** Transaction write entries. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxEntry<K, V>> writeEntries;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> writeEntriesBytes;

    /** Write entries which have not been transferred to nodes during lock request. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxEntry<K, V>> recoveryWrites;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> recoveryWritesBytes;

    /** Expected txSize. */
    private int txSize;

    /** Group lock key. */
    @GridDirectTransient
    private IgniteTxKey grpLockKey;

    /** Group lock key bytes. */
    private byte[] grpLockKeyBytes;

    /** System flag. */
    private boolean sys;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDistributedTxFinishRequest() {
        /* No-op. */
    }

    /**
     * @param xidVer Transaction ID.
     * @param futId future ID.
     * @param threadId Thread ID.
     * @param commitVer Commit version.
     * @param commit Commit flag.
     * @param invalidate Invalidate flag.
     * @param sys System flag.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param txSize Expected transaction size.
     * @param writeEntries Write entries.
     * @param recoveryWrites Recover entries. In pessimistic mode entries which were not transferred to remote nodes
     *      with lock requests. {@code Null} for optimistic mode.
     * @param grpLockKey Group lock key if this is a group-lock transaction.
     */
    public GridDistributedTxFinishRequest(
        GridCacheVersion xidVer,
        IgniteUuid futId,
        @Nullable GridCacheVersion commitVer,
        long threadId,
        boolean commit,
        boolean invalidate,
        boolean sys,
        boolean syncCommit,
        boolean syncRollback,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        int txSize,
        Collection<IgniteTxEntry<K, V>> writeEntries,
        Collection<IgniteTxEntry<K, V>> recoveryWrites,
        @Nullable IgniteTxKey grpLockKey
    ) {
        super(xidVer, writeEntries == null ? 0 : writeEntries.size());
        assert xidVer != null;

        this.futId = futId;
        this.commitVer = commitVer;
        this.threadId = threadId;
        this.commit = commit;
        this.invalidate = invalidate;
        this.sys = sys;
        this.syncCommit = syncCommit;
        this.syncRollback = syncRollback;
        this.baseVer = baseVer;
        this.txSize = txSize;
        this.writeEntries = writeEntries;
        this.recoveryWrites = recoveryWrites;
        this.grpLockKey = grpLockKey;

        completedVersions(committedVers, rolledbackVers);
    }

    /**
     * Clones write entries so that near entries are not passed to DHT cache.
     */
    public void cloneEntries() {
        if (F.isEmpty(writeEntries))
            return;

        Collection<IgniteTxEntry<K, V>> cp = new ArrayList<>(writeEntries.size());

        for (IgniteTxEntry<K, V> e : writeEntries) {
            GridCacheContext<K, V> cacheCtx = e.context();

            // Clone only if it is a near cache.
            if (cacheCtx.isNear())
                cp.add(e.cleanCopy(cacheCtx.nearTx().dht().context()));
            else
                cp.add(e);
        }

        writeEntries = cp;
    }

    /**
     * @return System flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Commit version.
     */
    public GridCacheVersion commitVersion() {
        return commitVer;
    }

    /**
     * @return Commit flag.
     */
    public boolean commit() {
        return commit;
    }

    /**
     *
     * @return Invalidate flag.
     */
    public boolean isInvalidate() {
        return invalidate;
    }

    /**
     * @return Sync commit flag.
     */
    public boolean syncCommit() {
        return syncCommit;
    }

    /**
     * @return Sync rollback flag.
     */
    public boolean syncRollback() {
        return syncRollback;
    }

    /**
     * @return Base version.
     */
    public GridCacheVersion baseVersion() {
        return baseVer;
    }

    /**
     * @return Write entries.
     */
    public Collection<IgniteTxEntry<K, V>> writes() {
        return writeEntries;
    }

    /**
     * @return Recover entries.
     */
    public Collection<IgniteTxEntry<K, V>> recoveryWrites() {
        return recoveryWrites;
    }

    /**
     * @return Expected tx size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     *
     * @return {@code True} if reply is required.
     */
    public boolean replyRequired() {
        return commit ? syncCommit : syncRollback;
    }

    /**
     * @return {@code True} if group lock transaction.
     */
    public boolean groupLock() {
        return grpLockKey != null;
    }

    /**
     * @return Group lock key.
     */
    @Nullable public IgniteTxKey groupLockKey() {
        return grpLockKey;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (writeEntries != null) {
            marshalTx(writeEntries, ctx);

            writeEntriesBytes = new ArrayList<>(writeEntries.size());

            for (IgniteTxEntry<K, V> e : writeEntries)
                writeEntriesBytes.add(ctx.marshaller().marshal(e));
        }

        if (recoveryWrites != null) {
            marshalTx(recoveryWrites, ctx);

            recoveryWritesBytes = new ArrayList<>(recoveryWrites.size());

            for (IgniteTxEntry<K, V> e : recoveryWrites)
                recoveryWritesBytes.add(ctx.marshaller().marshal(e));
        }

        if (grpLockKey != null && grpLockKeyBytes == null) {
            if (ctx.deploymentEnabled())
                prepareObject(grpLockKey, ctx);

            grpLockKeyBytes = CU.marshal(ctx, grpLockKey);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (writeEntriesBytes != null) {
            writeEntries = new ArrayList<>(writeEntriesBytes.size());

            for (byte[] arr : writeEntriesBytes)
                writeEntries.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(writeEntries, false, ctx, ldr);
        }

        if (recoveryWritesBytes != null) {
            recoveryWrites = new ArrayList<>(recoveryWritesBytes.size());

            for (byte[] arr : recoveryWritesBytes)
                recoveryWrites.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(recoveryWrites, false, ctx, ldr);
        }

        if (grpLockKeyBytes != null && grpLockKey == null)
            grpLockKey = ctx.marshaller().unmarshal(grpLockKeyBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors",
        "OverriddenMethodCallDuringObjectConstruction"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDistributedTxFinishRequest _clone = new GridDistributedTxFinishRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedTxFinishRequest _clone = (GridDistributedTxFinishRequest)_msg;

        _clone.futId = futId;
        _clone.threadId = threadId;
        _clone.commitVer = commitVer;
        _clone.invalidate = invalidate;
        _clone.commit = commit;
        _clone.baseVer = baseVer;
        _clone.writeEntries = writeEntries;
        _clone.writeEntriesBytes = writeEntriesBytes;
        _clone.recoveryWrites = recoveryWrites;
        _clone.recoveryWritesBytes = recoveryWritesBytes;
        _clone.txSize = txSize;
        _clone.grpLockKey = grpLockKey;
        _clone.grpLockKeyBytes = grpLockKeyBytes;
        _clone.sys = sys;
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
            case 8:
                if (!commState.putCacheVersion(baseVer))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putBoolean(commit))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putCacheVersion(commitVer))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putByteArray(grpLockKeyBytes))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putBoolean(invalidate))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putBoolean(syncCommit))
                    return false;

                commState.idx++;

            case 15:
                if (!commState.putBoolean(syncRollback))
                    return false;

                commState.idx++;

            case 16:
                if (recoveryWritesBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(recoveryWritesBytes.size()))
                            return false;

                        commState.it = recoveryWritesBytes.iterator();
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

            case 17:
                if (!commState.putLong(threadId))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putInt(txSize))
                    return false;

                commState.idx++;

            case 19:
                if (writeEntriesBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(writeEntriesBytes.size()))
                            return false;

                        commState.it = writeEntriesBytes.iterator();
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

            case 20:
                if (!commState.putBoolean(sys))
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
            case 8:
                GridCacheVersion baseVer0 = commState.getCacheVersion();

                if (baseVer0 == CACHE_VER_NOT_READ)
                    return false;

                baseVer = baseVer0;

                commState.idx++;

            case 9:
                if (buf.remaining() < 1)
                    return false;

                commit = commState.getBoolean();

                commState.idx++;

            case 10:
                GridCacheVersion commitVer0 = commState.getCacheVersion();

                if (commitVer0 == CACHE_VER_NOT_READ)
                    return false;

                commitVer = commitVer0;

                commState.idx++;

            case 11:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 12:
                byte[] grpLockKeyBytes0 = commState.getByteArray();

                if (grpLockKeyBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                grpLockKeyBytes = grpLockKeyBytes0;

                commState.idx++;

            case 13:
                if (buf.remaining() < 1)
                    return false;

                invalidate = commState.getBoolean();

                commState.idx++;

            case 14:
                if (buf.remaining() < 1)
                    return false;

                syncCommit = commState.getBoolean();

                commState.idx++;

            case 15:
                if (buf.remaining() < 1)
                    return false;

                syncRollback = commState.getBoolean();

                commState.idx++;

            case 16:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (recoveryWritesBytes == null)
                        recoveryWritesBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        recoveryWritesBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 17:
                if (buf.remaining() < 8)
                    return false;

                threadId = commState.getLong();

                commState.idx++;

            case 18:
                if (buf.remaining() < 4)
                    return false;

                txSize = commState.getInt();

                commState.idx++;

            case 19:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (writeEntriesBytes == null)
                        writeEntriesBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        writeEntriesBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 20:
                if (buf.remaining() < 1)
                    return false;

                sys = commState.getBoolean();

                commState.idx++;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishRequest.class, this,
            "super", super.toString());
    }
}
