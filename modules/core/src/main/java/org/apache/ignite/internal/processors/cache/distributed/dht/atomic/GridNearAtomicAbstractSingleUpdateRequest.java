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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearAtomicAbstractSingleUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final CacheEntryPredicate[] NO_FILTER = new CacheEntryPredicate[0];

    /** Fast map flag mask. */
    private static final int FAST_MAP_FLAG_MASK = 0x1;

    /** Flag indicating whether request contains primary keys. */
    private static final int HAS_PRIMARY_FLAG_MASK = 0x2;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    private static final int TOP_LOCKED_FLAG_MASK = 0x4;

    /** Skip write-through to a persistent storage. */
    private static final int SKIP_STORE_FLAG_MASK = 0x8;

    /** */
    private static final int CLIENT_REQ_FLAG_MASK = 0x10;

    /** Keep binary flag. */
    private static final int KEEP_BINARY_FLAG_MASK = 0x20;

    /** Return value flag. */
    private static final int RET_VAL_FLAG_MASK = 0x40;

    /** Target node ID. */
    @GridDirectTransient
    protected UUID nodeId;

    /** Future version. */
    protected GridCacheVersion futVer;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** Write synchronization mode. */
    protected CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    protected GridCacheOperation op;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /** */
    @GridDirectTransient
    private GridNearAtomicUpdateResponse res;

    /** Compressed boolean flags. */
    protected byte flags;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    protected GridNearAtomicAbstractSingleUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param fastMap Fast map scheme flag.
     * @param updateVer Update version set if fast map is performed.
     * @param topVer Topology version.
     * @param topLocked Topology locked flag.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     */
    protected GridNearAtomicAbstractSingleUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean topLocked,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        boolean clientReq,
        boolean addDepInfo
    ) {
        assert futVer != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.updateVer = updateVer;
        this.topVer = topVer;
        this.syncMode = syncMode;
        this.op = op;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.addDepInfo = addDepInfo;

        fastMap(fastMap);
        topologyLocked(topLocked);
        returnValue(retval);
        skipStore(skipStore);
        keepBinary(keepBinary);
        clientRequest(clientReq);
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Mapped node ID.
     */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    @Override public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Subject ID.
     */
    @Override public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Future version.
     */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Update version for fast-map request.
     */
    @Override public GridCacheVersion updateVersion() {
        return updateVer;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Expiry policy.
     */
    @Override public ExpiryPolicy expiry() {
        return null;
    }

    /**
     * @return Update operation.
     */
    @Override public GridCacheOperation operation() {
        return op;
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Override @Nullable public Object[] invokeArguments() {
        return null;
    }

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    @Override public boolean onResponse(GridNearAtomicUpdateResponse res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /**
     * @return Response.
     */
    @Override @Nullable public GridNearAtomicUpdateResponse response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    @Override public boolean fastMap() {
        return isFlag(FAST_MAP_FLAG_MASK);
    }

    /**
     * Sets fastMap flag value.
     */
    public void fastMap(boolean val) {
        setFlag(val, FAST_MAP_FLAG_MASK);
    }

    /**
     * @return Topology locked flag.
     */
    @Override public boolean topologyLocked() {
        return isFlag(TOP_LOCKED_FLAG_MASK);
    }

    /**
     * Sets topologyLocked flag value.
     */
    public void topologyLocked(boolean val) {
        setFlag(val, TOP_LOCKED_FLAG_MASK);
    }

    /**
     * @return {@code True} if request sent from client node.
     */
    @Override public boolean clientRequest() {
        return isFlag(CLIENT_REQ_FLAG_MASK);
    }

    /**
     * Sets clientRequest flag value.
     */
    public void clientRequest(boolean val) {
        setFlag(val, CLIENT_REQ_FLAG_MASK);
    }

    /**
     * @return Return value flag.
     */
    @Override public boolean returnValue() {
        return isFlag(RET_VAL_FLAG_MASK);
    }

    /**
     * Sets returnValue flag value.
     */
    public void returnValue(boolean val) {
        setFlag(val, RET_VAL_FLAG_MASK);
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    @Override public boolean skipStore() {
        return isFlag(SKIP_STORE_FLAG_MASK);
    }

    /**
     * Sets skipStore flag value.
     */
    public void skipStore(boolean val) {
        setFlag(val, SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return Keep binary flag.
     */
    @Override public boolean keepBinary() {
        return isFlag(KEEP_BINARY_FLAG_MASK);
    }

    /**
     * Sets keepBinary flag value.
     */
    public void keepBinary(boolean val) {
        setFlag(val, KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    @Override public boolean hasPrimary() {
        return isFlag(HAS_PRIMARY_FLAG_MASK);
    }

    /**
     * Sets hasPrimary flag value.
     */
    public void hasPrimary(boolean val) {
        setFlag(val, HAS_PRIMARY_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntryPredicate[] filter() {
        return NO_FILTER;
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
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
            case 3:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("updateVer", updateVer))
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
            case 3:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 6:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 8:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicAbstractSingleUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 11;
    }
}
