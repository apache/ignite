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
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Transaction completion message.
 */
public class GridDistributedTxFinishRequest extends GridDistributedBaseMessage {
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

    /** Expected txSize. */
    private int txSize;

    /** Group lock key. */
    private IgniteTxKey grpLockKey;

    /** System transaction flag. */
    private boolean sys;

    /** IO policy. */
    private GridIoPolicy plc;

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
     * @param sys System transaction flag.
     * @param plc IO policy.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param txSize Expected transaction size.
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
        GridIoPolicy plc,
        boolean syncCommit,
        boolean syncRollback,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        int txSize,
        @Nullable IgniteTxKey grpLockKey
    ) {
        super(xidVer, 0);
        assert xidVer != null;

        this.futId = futId;
        this.commitVer = commitVer;
        this.threadId = threadId;
        this.commit = commit;
        this.invalidate = invalidate;
        this.sys = sys;
        this.plc = plc;
        this.syncCommit = syncCommit;
        this.syncRollback = syncRollback;
        this.baseVer = baseVer;
        this.txSize = txSize;
        this.grpLockKey = grpLockKey;

        completedVersions(committedVers, rolledbackVers);
    }

    /**
     * @return System transaction flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @return IO policy.
     */
    public GridIoPolicy policy() {
        return plc;
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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (grpLockKey != null)
            grpLockKey.prepareMarshal(ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (grpLockKey != null)
            grpLockKey.finishUnmarshal(ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeMessage("baseVer", baseVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("commit", commit))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("commitVer", commitVer))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMessage("grpLockKey", grpLockKey))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeBoolean("invalidate", invalidate))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByte("plc", plc != null ? (byte)plc.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeBoolean("syncCommit", syncCommit))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeBoolean("syncRollback", syncRollback))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeBoolean("sys", sys))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeInt("txSize", txSize))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 8:
                baseVer = reader.readMessage("baseVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                commit = reader.readBoolean("commit");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                commitVer = reader.readMessage("commitVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                grpLockKey = reader.readMessage("grpLockKey");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                invalidate = reader.readBoolean("invalidate");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                byte plcOrd;

                plcOrd = reader.readByte("plc");

                if (!reader.isLastRead())
                    return false;

                plc = GridIoPolicy.fromOrdinal(plcOrd);

                reader.incrementState();

            case 15:
                syncCommit = reader.readBoolean("syncCommit");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                syncRollback = reader.readBoolean("syncRollback");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                sys = reader.readBoolean("sys");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                txSize = reader.readInt("txSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 23;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishRequest.class, this,
            "super", super.toString());
    }
}
