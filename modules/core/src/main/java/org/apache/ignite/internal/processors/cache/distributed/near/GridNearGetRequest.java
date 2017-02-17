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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 * Get request.
 */
public class GridNearGetRequest extends GridCacheMessage implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Sub ID. */
    private IgniteUuid miniId;

    /** Version. */
    private GridCacheVersion ver;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private LinkedHashMap<KeyCacheObject, Boolean> keyMap;

    /** */
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Partition IDs. */
    @GridDirectCollection(int.class)
    private List<Integer> partIds;

    /** */
    @GridDirectCollection(boolean.class)
    private Collection<Boolean> flags;

    /** Reload flag. */
    private boolean reload;

    /** Read through flag. */
    private boolean readThrough;

    /** Skip values flag. Used for {@code containsKey} method. */
    private boolean skipVals;

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

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearGetRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param skipVals Skip values flag. When false, only boolean values will be returned indicating whether
     *      cache entry has a value.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param createTtl New TTL to set after entry is created, -1 to leave unchanged.
     * @param accessTtl New TTL to set after entry is accessed, -1 to leave unchanged.
     * @param addDepInfo Deployment info.
     */
    public GridNearGetRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        Map<KeyCacheObject, Boolean> keys,
        boolean readThrough,
        @NotNull AffinityTopologyVersion topVer,
        UUID subjId,
        int taskNameHash,
        long createTtl,
        long accessTtl,
        boolean skipVals,
        boolean addDepInfo
    ) {
        assert futId != null;
        assert miniId != null;
        assert keys != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;

        this.keys = new ArrayList<>(keys.size());
        flags = new ArrayList<>(keys.size());
        partIds = new ArrayList<>(keys.size());

        for (Map.Entry<KeyCacheObject, Boolean> entry : keys.entrySet()) {
            this.keys.add(entry.getKey());
            flags.add(entry.getValue());
            partIds.add(entry.getKey().partition());
        }

        this.readThrough = readThrough;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;
        this.skipVals = skipVals;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Sub ID.
     */
    public IgniteUuid miniId() {
        return miniId;
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

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Keys
     */
    public LinkedHashMap<KeyCacheObject, Boolean> keys() {
        return keyMap;
    }

    /**
     * @return Reload flag.
     */
    public boolean reload() {
        return reload;
    }

    /**
     * @return Read through flag.
     */
    public boolean readThrough() {
        return readThrough;
    }

    /**
     * @return Skip values flag. If true, boolean values indicating whether cache entry has a value will be
     *      returned as future result.
     */
    public boolean skipValues() {
        return skipVals;
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
        return partIds != null && !partIds.isEmpty() ? partIds.get(0) : -1;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert ctx != null;
        assert !F.isEmpty(keys);
        assert keys.size() == flags.size();

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        assert !F.isEmpty(keys);
        assert keys.size() == flags.size();

        if (keyMap == null) {
            keyMap = U.newLinkedHashMap(keys.size());

            Iterator<KeyCacheObject> keysIt = keys.iterator();
            Iterator<Boolean> flagsIt = flags.iterator();

            while (keysIt.hasNext())
                keyMap.put(keysIt.next(), flagsIt.next());
        }

        if (partIds != null && !partIds.isEmpty()) {
            assert partIds.size() == keys.size();

            for (int i = 0; i < keys.size(); i++)
                keys.get(i).partition(partIds.get(i));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
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
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("createTtl", createTtl))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("flags", flags, MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("partIds", partIds, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBoolean("readThrough", readThrough))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("reload", reload))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean("skipVals", skipVals))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMessage("ver", ver))
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
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                createTtl = reader.readLong("createTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                flags = reader.readCollection("flags", MessageCollectionItemType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                partIds = reader.readCollection("partIds", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                readThrough = reader.readBoolean("readThrough");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                reload = reader.readBoolean("reload");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                skipVals = reader.readBoolean("skipVals");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearGetRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 49;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetRequest.class, this);
    }
}
