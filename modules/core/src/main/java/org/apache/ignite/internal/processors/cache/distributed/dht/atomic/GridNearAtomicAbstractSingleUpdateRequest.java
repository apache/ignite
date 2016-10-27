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
import org.apache.ignite.IgniteCheckedException;
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

    /** Fast map flag. */
    @GridDirectTransient
    protected boolean fastMap;

    /** Flag indicating whether request contains primary keys. */
    @GridDirectTransient
    protected boolean hasPrimary;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    @GridDirectTransient
    protected boolean topLocked;

    /** Skip write-through to a persistent storage. */
    @GridDirectTransient
    protected boolean skipStore;

    /** */
    @GridDirectTransient
    protected boolean clientReq;

    /** Keep binary flag. */
    @GridDirectTransient
    protected boolean keepBinary;

    /** Return value flag. */
    @GridDirectTransient
    protected boolean retval;

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
     * @param filter Optional filter for atomic check.
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
        @Nullable CacheEntryPredicate[] filter,
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
            filter,
            subjId,
            taskNameHash,
            addDepInfo
        );

        this.fastMap = fastMap;
        this.topLocked = topLocked;
        this.retval = retval;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.clientReq = clientReq;
    }

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    @Override public boolean fastMap() {
        return fastMap;
    }

    /**
     * @return Topology locked flag.
     */
    @Override public boolean topologyLocked() {
        return topLocked;
    }

    /**
     * @return {@code True} if request sent from client node.
     */
    @Override public boolean clientRequest() {
        return clientReq;
    }

    /**
     * @return Return value flag.
     */
    @Override public boolean returnValue() {
        return retval;
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /**
     * @return Keep binary flag.
     */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    @Override public boolean hasPrimary() {
        return hasPrimary;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        flags = (byte)(
            (fastMap ? 1 : 0) +
                (hasPrimary ? 1 << 1 : 0) +
                (topLocked ? 1 << 2 : 0) +
                (skipStore ? 1 << 3 : 0) +
                (clientReq ? 1 << 4 : 0) +
                (keepBinary ? 1 << 5 : 0) +
                (retval ? 1 << 6 : 0)
        );
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        fastMap = (flags & 1) > 0;
        hasPrimary = (flags & 1 << 1) > 0;
        topLocked = (flags & 1 << 2) > 0;
        skipStore = (flags & 1 << 3) > 0;
        clientReq = (flags & 1 << 4) > 0;
        keepBinary = (flags & 1 << 5) > 0;
        retval = (flags & 1 << 6) > 0;
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
            case 11:
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
            case 11:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicAbstractSingleUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
    }
}
