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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Near transaction finish request.
 */
public class GridNearTxFinishRequest extends GridDistributedTxFinishRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Mini future ID. */
    private int miniId;

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
     * @param sys System flag.
     * @param plc IO policy.
     * @param syncMode Write synchronization mode.
     * @param explicitLock Explicit lock flag.
     * @param storeEnabled Store enabled flag.
     * @param topVer Topology version.
     * @param baseVer Base version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param txSize Expected transaction size.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearTxFinishRequest(
        IgniteUuid futId,
        GridCacheVersion xidVer,
        long threadId,
        boolean commit,
        boolean invalidate,
        boolean sys,
        byte plc,
        CacheWriteSynchronizationMode syncMode,
        boolean explicitLock,
        boolean storeEnabled,
        @NotNull AffinityTopologyVersion topVer,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean addDepInfo) {
        super(
            xidVer,
            futId,
            topVer,
            null,
            threadId,
            commit,
            invalidate,
            sys,
            plc,
            syncMode,
            baseVer,
            committedVers,
            rolledbackVers,
            subjId,
            taskNameHash,
            txSize,
            addDepInfo
        );

        explicitLock(explicitLock);
        storeEnabled(storeEnabled);
    }

    /**
     * @return Explicit lock flag.
     */
    public boolean explicitLock() {
        return isFlag(EXPLICIT_LOCK_FLAG_MASK);
    }

    /**
     * @param explicitLock Explicit lock flag.
     */
    private void explicitLock(boolean explicitLock) {
        setFlag(explicitLock, EXPLICIT_LOCK_FLAG_MASK);
    }

    /**
     * @return Store enabled flag.
     */
    public boolean storeEnabled() {
        return isFlag(STORE_ENABLED_FLAG_MASK);
    }

    /**
     * @param storeEnabled Store enabled flag.
     */
    private void storeEnabled(boolean storeEnabled) {
        setFlag(storeEnabled, STORE_ENABLED_FLAG_MASK);
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(int miniId) {
        assert miniId > 0;

        this.miniId = miniId;
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
            case 21:
                if (!writer.writeInt("miniId", miniId))
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
            case 21:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxFinishRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 53;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 22;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return U.safeAbs(version().hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridNearTxFinishRequest.class, this, "super", super.toString());
    }
}
