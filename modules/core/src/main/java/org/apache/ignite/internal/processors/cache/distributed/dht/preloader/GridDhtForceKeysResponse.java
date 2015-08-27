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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Force keys response. Contains absent keys.
 */
public class GridDhtForceKeysResponse extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini-future ID. */
    private IgniteUuid miniId;

    /** Error. */
    @GridDirectTransient
    private volatile IgniteCheckedException err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Missed (not found) keys. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> missedKeys;

    /** Cache entries. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheEntryInfo.class)
    private List<GridCacheEntryInfo> infos;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtForceKeysResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Request id.
     * @param miniId Mini-future ID.
     */
    public GridDhtForceKeysResponse(int cacheId, IgniteUuid futId, IgniteUuid miniId) {
        assert futId != null;
        assert miniId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
    }

    /**
     * Sets error.
     * @param err Error.
     */
    public void error(IgniteCheckedException err){
        this.err = err;
    }

    /**
     * @return Error, if any.
     */
    public IgniteCheckedException error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Keys.
     */
    public Collection<KeyCacheObject> missedKeys() {
        return missedKeys == null ? Collections.<KeyCacheObject>emptyList() : missedKeys;
    }

    /**
     * @return Forced entries.
     */
    public Collection<GridCacheEntryInfo> forcedInfos() {
        return infos == null ? Collections.<GridCacheEntryInfo>emptyList() : infos;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini-future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param key Key.
     */
    public void addMissed(KeyCacheObject key) {
        if (missedKeys == null)
            missedKeys = new ArrayList<>();

        missedKeys.add(key);
    }

    /**
     * @param info Entry info to add.
     */
    public void addInfo(GridCacheEntryInfo info) {
        assert info != null;

        if (infos == null)
            infos = new ArrayList<>();

        infos.add(info);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (missedKeys != null)
            prepareMarshalCacheObjects(missedKeys, cctx);

        if (infos != null) {
            for (GridCacheEntryInfo info : infos)
                info.marshal(cctx);
        }

        errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (missedKeys != null)
            finishUnmarshalCacheObjects(missedKeys, cctx, ldr);

        if (infos != null) {
            for (GridCacheEntryInfo info : infos)
                info.unmarshal(cctx, ldr);
        }

        err = ctx.marshaller().unmarshal(errBytes, ldr);
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
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("infos", infos, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("missedKeys", missedKeys, MessageCollectionItemType.MSG))
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
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                infos = reader.readCollection("infos", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                missedKeys = reader.readCollection("missedKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtForceKeysResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 43;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtForceKeysResponse.class, this, super.toString());
    }
}
