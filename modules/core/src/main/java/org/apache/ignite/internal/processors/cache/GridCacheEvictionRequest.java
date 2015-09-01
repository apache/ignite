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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 * Cache eviction request.
 */
public class GridCacheEvictionRequest extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future id. */
    private long futId;

    /** Entries to clear from near and backup nodes. */
    @GridToStringInclude
    @GridDirectCollection(CacheEvictionEntry.class)
    private Collection<CacheEvictionEntry> entries;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheEvictionRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future id.
     * @param size Size.
     * @param topVer Topology version.
     */
    GridCacheEvictionRequest(int cacheId, long futId, int size, @NotNull AffinityTopologyVersion topVer) {
        assert futId > 0;
        assert size > 0;
        assert topVer.topologyVersion() > 0;

        this.cacheId = cacheId;
        this.futId = futId;

        entries = new ArrayList<>(size);

        this.topVer = topVer;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (entries != null) {
            boolean depEnabled = ctx.deploymentEnabled();

            GridCacheContext cctx = ctx.cacheContext(cacheId);

            for (CacheEvictionEntry e : entries) {
                e.prepareMarshal(cctx);

                if (depEnabled)
                    prepareObject(e.key().value(cctx.cacheObjectContext(), false), ctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (entries != null) {
            GridCacheContext cctx = ctx.cacheContext(cacheId);

            for (CacheEvictionEntry e : entries)
                e.finishUnmarshal(cctx, ldr);
        }
    }

    /**
     * @return Future id.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return Entries - {{Key, Version, Boolean (near or not)}, ...}.
     */
    Collection<CacheEvictionEntry> entries() {
        return entries;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Add key to request.
     *
     * @param key Key to evict.
     * @param ver Entry version.
     * @param near {@code true} if key should be evicted from near cache.
     */
    void addKey(KeyCacheObject key, GridCacheVersion ver, boolean near) {
        assert key != null;
        assert ver != null;

        entries.add(new CacheEvictionEntry(key, ver, near));
    }

    /** {@inheritDoc} */
    @Override public boolean ignoreClassErrors() {
        return true;
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
                if (!writer.writeCollection("entries", entries, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
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
                entries = reader.readCollection("entries", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheEvictionRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 14;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionRequest.class, this);
    }
}