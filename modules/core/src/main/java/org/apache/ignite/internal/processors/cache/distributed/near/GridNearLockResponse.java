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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Near cache lock response.
 */
public class GridNearLockResponse extends GridDistributedLockResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of versions that are pending and less than lock version. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pending;

    /** */
    private IgniteUuid miniId;

    /** DHT versions. */
    @GridToStringInclude
    private GridCacheVersion[] dhtVers;

    /** DHT candidate versions. */
    @GridToStringInclude
    private GridCacheVersion[] mappedVers;

    /** Filter evaluation results for fast-commit transactions. */
    private boolean[] filterRes;

    /** {@code True} if client node should remap lock request. */
    private AffinityTopologyVersion clientRemapVer;

    /**
     * Empty constructor (required by {@link Externalizable}).
     */
    public GridNearLockResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param filterRes {@code True} if need to allocate array for filter evaluation results.
     * @param cnt Count.
     * @param err Error.
     * @param clientRemapVer {@code True} if client node should remap lock request.
     */
    public GridNearLockResponse(
        int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean filterRes,
        int cnt,
        Throwable err,
        AffinityTopologyVersion clientRemapVer
    ) {
        super(cacheId, lockVer, futId, cnt, err);

        assert miniId != null;

        this.miniId = miniId;
        this.clientRemapVer = clientRemapVer;

        dhtVers = new GridCacheVersion[cnt];
        mappedVers = new GridCacheVersion[cnt];

        if (filterRes)
            this.filterRes = new boolean[cnt];
    }

    /**
     * @return {@code True} if client node should remap lock request.
     */
    @Nullable public AffinityTopologyVersion clientRemapVersion() {
        return clientRemapVer;
    }

    /**
     * Gets pending versions that are less than {@link #version()}.
     *
     * @return Pending versions.
     */
    public Collection<GridCacheVersion> pending() {
        return pending;
    }

    /**
     * Sets pending versions that are less than {@link #version()}.
     *
     * @param pending Pending versions.
     */
    public void pending(Collection<GridCacheVersion> pending) {
        this.pending = pending;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param idx Index.
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion(int idx) {
        return dhtVers == null ? null : dhtVers[idx];
    }

    /**
     * Returns DHT candidate version for acquired near lock on DHT node.
     *
     * @param idx Key index.
     * @return DHT version.
     */
    public GridCacheVersion mappedVersion(int idx) {
        return mappedVers == null ? null : mappedVers[idx];
    }

    /**
     * Gets filter evaluation result for fast-commit transaction.
     *
     * @param idx Result index.
     * @return {@code True} if filter passed on primary node, {@code false} otherwise.
     */
    public boolean filterResult(int idx) {
        assert filterRes != null : "Should not call filterResult for non-fast-commit transactions.";

        return filterRes[idx];
    }

    /**
     * @param val Value.
     * @param filterPassed Boolean flag indicating whether filter passed for fast-commit transaction.
     * @param dhtVer DHT version.
     * @param mappedVer Mapped version.
     * @throws IgniteCheckedException If failed.
     */
    public void addValueBytes(
        @Nullable CacheObject val,
        boolean filterPassed,
        @Nullable GridCacheVersion dhtVer,
        @Nullable GridCacheVersion mappedVer
    ) throws IgniteCheckedException {
        int idx = valuesSize();

        dhtVers[idx] = dhtVer;
        mappedVers[idx] = mappedVer;

        if (filterRes != null)
            filterRes[idx] = filterPassed;

        // Delegate to super.
        addValue(val);
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
                if (!writer.writeMessage("clientRemapVer", clientRemapVer))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeObjectArray("dhtVers", dhtVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBooleanArray("filterRes", filterRes))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeObjectArray("mappedVers", mappedVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection("pending", pending, MessageCollectionItemType.MSG))
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
                clientRemapVer = reader.readMessage("clientRemapVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                dhtVers = reader.readObjectArray("dhtVers", MessageCollectionItemType.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                filterRes = reader.readBooleanArray("filterRes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                mappedVers = reader.readObjectArray("mappedVers", MessageCollectionItemType.MSG, GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                pending = reader.readCollection("pending", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearLockResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 52;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockResponse.class, this, super.toString());
    }
}