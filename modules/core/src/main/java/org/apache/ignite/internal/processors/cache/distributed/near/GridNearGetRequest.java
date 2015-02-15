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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Get request.
 */
public class GridNearGetRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable,
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
    private LinkedHashMap<K, Boolean> keys;

    /** Reload flag. */
    private boolean reload;

    /** Read through flag. */
    private boolean readThrough;

    /** Skip values flag. Used for {@code containsKey} method. */
    private boolean skipVals;

    /** */
    @GridToStringExclude
    @GridDirectMap(keyType = byte[].class, valueType = boolean.class)
    private LinkedHashMap<byte[], Boolean> keyBytes;

    /** Topology version. */
    private long topVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

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
     * @param reload Reload flag.
     * @param skipVals Skip values flag. When false, only boolean values will be returned indicating whether
     *      cache entry has a value.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param accessTtl New TTL to set after entry is accessed, -1 to leave unchanged.
     */
    public GridNearGetRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        LinkedHashMap<K, Boolean> keys,
        boolean readThrough,
        boolean reload,
        long topVer,
        UUID subjId,
        int taskNameHash,
        long accessTtl,
        boolean skipVals
    ) {
        assert futId != null;
        assert miniId != null;
        assert ver != null;
        assert keys != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;
        this.keys = keys;
        this.readThrough = readThrough;
        this.reload = reload;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.accessTtl = accessTtl;
        this.skipVals = skipVals;
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
    public LinkedHashMap<K, Boolean> keys() {
        return keys;
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
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return New TTL to set after entry is accessed, -1 to leave unchanged.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert ctx != null;
        assert !F.isEmpty(keys);

        if (keyBytes == null)
            keyBytes = marshalBooleanLinkedMap(keys, ctx);
    }

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null)
            keys = unmarshalBooleanLinkedMap(keyBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridNearGetRequest _clone = (GridNearGetRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.ver = ver != null ? (GridCacheVersion)ver.clone() : null;
        _clone.keys = keys;
        _clone.reload = reload;
        _clone.readThrough = readThrough;
        _clone.skipVals = skipVals;
        _clone.keyBytes = keyBytes;
        _clone.topVer = topVer;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
        _clone.accessTtl = accessTtl;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeLong("accessTtl", accessTtl))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap("keyBytes", keyBytes, Type.BYTE_ARR, Type.BOOLEAN))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("readThrough", readThrough))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeBoolean("reload", reload))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("skipVals", skipVals))
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
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("ver", ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                accessTtl = reader.readLong("accessTtl");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                keyBytes = reader.readMap("keyBytes", Type.BYTE_ARR, Type.BOOLEAN, true);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                readThrough = reader.readBoolean("readThrough");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                reload = reader.readBoolean("reload");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                skipVals = reader.readBoolean("skipVals");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 49;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetRequest.class, this);
    }
}
