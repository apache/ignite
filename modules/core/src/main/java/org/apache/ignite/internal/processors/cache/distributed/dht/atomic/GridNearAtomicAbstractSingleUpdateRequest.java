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
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
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

    /** compressed boolean flags */
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
        super(
            cacheId,
            nodeId,
            futVer,
            updateVer,
            topVer,
            syncMode,
            op,
            subjId,
            taskNameHash,
            addDepInfo
        );

        fastMap(fastMap);
        topologyLocked(topLocked);
        returnValue(retval);
        skipStore(skipStore);
        keepBinary(keepBinary);
        clientRequest(clientReq);
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
            case 10:
                if (!writer.writeByte("flags", flags))
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
            case 10:
                flags = reader.readByte("flags");

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
