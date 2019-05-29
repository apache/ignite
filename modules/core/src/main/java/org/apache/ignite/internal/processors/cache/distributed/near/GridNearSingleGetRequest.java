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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridNearSingleGetRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int READ_THROUGH_FLAG_MASK = 0x01;

    /** */
    private static final int SKIP_VALS_FLAG_MASK = 0x02;

    /** */
    private static final int ADD_READER_FLAG_MASK = 0x04;

    /** */
    private static final int NEED_VER_FLAG_MASK = 0x08;

    /** */
    private static final int NEED_ENTRY_INFO_FLAG_MASK = 0x10;

    /** */
    public static final int RECOVERY_FLAG_MASK = 0x20;

    /** Future ID. */
    private long futId;

    /** */
    private KeyCacheObject key;

    /** Flags. */
    private byte flags;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** TTL for read operation. */
    private long createTtl;

    /** TTL for read operation. */
    private long accessTtl;

    /** Transaction label. */
    private @Nullable String txLbl;

    /** */
    private MvccSnapshot mvccSnapshot;

    /**
     * Empty constructor required for {@link Message}.
     */
    public GridNearSingleGetRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param key Key.
     * @param readThrough Read through flag.
     * @param skipVals Skip values flag. When false, only boolean values will be returned indicating whether
     *      cache entry has a value.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param createTtl New TTL to set after entry is created, -1 to leave unchanged.
     * @param accessTtl New TTL to set after entry is accessed, -1 to leave unchanged.
     * @param addReader Add reader flag.
     * @param needVer {@code True} if entry version is needed.
     * @param addDepInfo Deployment info.
     * @param txLbl Transaction label.
     * @param mvccSnapshot MVCC snapshot.
     */
    public GridNearSingleGetRequest(
        int cacheId,
        long futId,
        KeyCacheObject key,
        boolean readThrough,
        @NotNull AffinityTopologyVersion topVer,
        UUID subjId,
        int taskNameHash,
        long createTtl,
        long accessTtl,
        boolean skipVals,
        boolean addReader,
        boolean needVer,
        boolean addDepInfo,
        boolean recovery,
        @Nullable String txLbl,
        MvccSnapshot mvccSnapshot
    ) {
        assert key != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.key = key;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;
        this.addDepInfo = addDepInfo;
        this.txLbl = txLbl;
        this.mvccSnapshot = mvccSnapshot;

        if (readThrough)
            flags |= READ_THROUGH_FLAG_MASK;

        if (skipVals)
            flags |= SKIP_VALS_FLAG_MASK;

        if (addReader)
            flags |= ADD_READER_FLAG_MASK;

        if (needVer)
            flags |= NEED_VER_FLAG_MASK;

        if (recovery)
            flags |= RECOVERY_FLAG_MASK;
    }

    /**
     * @return Mvcc version.
     */
    @Nullable public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * Gets task name hash.
     *
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return New TTL to set after entry is created, -1 to leave unchanged.
     */
    public long createTtl() {
        return createTtl;
    }

    /**
     * @return New TTL to set after entry is accessed, -1 to leave unchanged.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        assert key != null;

        return key.partition();
    }

    /**
     * Get transaction label (may be null).
     *
     * @return Transaction label;
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /**
     * @return Read through flag.
     */
    public boolean readThrough() {
        return (flags & READ_THROUGH_FLAG_MASK) != 0;
    }

    /**
     * @return Read through flag.
     */
    public boolean skipValues() {
        return (flags & SKIP_VALS_FLAG_MASK) != 0;
    }

    /**
     * @return Add reader flag.
     */
    public boolean addReader() {
        return (flags & ADD_READER_FLAG_MASK) != 0;
    }

    /**
     * @return {@code True} if entry version is needed.
     */
    public boolean needVersion() {
        return (flags & NEED_VER_FLAG_MASK) != 0;
    }

    /**
     * @return {@code True} if full entry information is needed.
     */
    public boolean needEntryInfo() {
        return (flags & NEED_ENTRY_INFO_FLAG_MASK) != 0;
    }

    /**
     * @return {@code True} if recovery flag is set.
     */
    public boolean recovery() {
        return (flags & RECOVERY_FLAG_MASK) != 0;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert key != null;

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        assert key != null;

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        key.finishUnmarshal(cctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                createTtl = reader.readLong("createTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                txLbl = reader.readString("txLbl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearSingleGetRequest.class);
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
            case 4:
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("createTtl", createTtl))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeString("txLbl", txLbl))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 116;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 14;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearSingleGetRequest.class, this);
    }
}
