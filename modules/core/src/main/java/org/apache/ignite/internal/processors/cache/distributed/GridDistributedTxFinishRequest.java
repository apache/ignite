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
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeField("baseVer", baseVer, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("commit", commit, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeField("commitVer", commitVer, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeField("futId", futId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeField("grpLockKeyBytes", grpLockKeyBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeField("invalidate", invalidate, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollectionField("recoveryWritesBytes", recoveryWritesBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeField("syncCommit", syncCommit, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeField("syncRollback", syncRollback, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeField("sys", sys, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeField("threadId", threadId, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeField("txSize", txSize, MessageFieldType.INT))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeCollectionField("writeEntriesBytes", writeEntriesBytes, MessageFieldType.BYTE_ARR))
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
            case 8:
                baseVer = reader.readField("baseVer", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                commit = reader.readField("commit", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                commitVer = reader.readField("commitVer", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                futId = reader.readField("futId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                grpLockKeyBytes = reader.readField("grpLockKeyBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                invalidate = reader.readField("invalidate", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                recoveryWritesBytes = reader.readCollectionField("recoveryWritesBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 15:
                syncCommit = reader.readField("syncCommit", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 16:
                syncRollback = reader.readField("syncRollback", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 17:
                sys = reader.readField("sys", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 18:
                threadId = reader.readField("threadId", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 19:
                txSize = reader.readField("txSize", MessageFieldType.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 20:
                writeEntriesBytes = reader.readCollectionField("writeEntriesBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 23;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishRequest.class, this,
            "super", super.toString());
    }
}
