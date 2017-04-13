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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Force keys request. This message is sent by node while preloading to force
 * another node to put given keys into the next batch of transmitting entries.
 */
public class GridDhtForceKeysRequest extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini-future ID. */
    private IgniteUuid miniId;

    /** Keys to request. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private Collection<KeyCacheObject> keys;

    /** Topology version for which keys are requested. */
    private AffinityTopologyVersion topVer;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtForceKeysRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Mini-future ID.
     * @param keys Keys.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info.
     */
    GridDhtForceKeysRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        assert futId != null;
        assert miniId != null;
        assert !F.isEmpty(keys);

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.keys = keys;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
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
     * @return Keys.
     */
    public Collection<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @return Topology version for which keys are requested.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Key count.
     */
    private int keyCount() {
        return keys.size();
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
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("topVer", topVer))
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
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtForceKeysRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 42;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtForceKeysRequest.class, this, "keyCnt", keyCount(), "super", super.toString());
    }
}
