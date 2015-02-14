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
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near cache lock response.
 */
public class GridNearLockResponse<K, V> extends GridDistributedLockResponse<K, V> {
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
     */
    public GridNearLockResponse(
        int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean filterRes,
        int cnt,
        Throwable err
    ) {
        super(cacheId, lockVer, futId, cnt, err);

        assert miniId != null;

        this.miniId = miniId;

        dhtVers = new GridCacheVersion[cnt];
        mappedVers = new GridCacheVersion[cnt];

        if (filterRes)
            this.filterRes = new boolean[cnt];
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
     * @param valBytes Value bytes (possibly {@code null}).
     * @param filterPassed Boolean flag indicating whether filter passed for fast-commit transaction.
     * @param dhtVer DHT version.
     * @param mappedVer Mapped version.
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addValueBytes(
        @Nullable V val,
        @Nullable byte[] valBytes,
        boolean filterPassed,
        @Nullable GridCacheVersion dhtVer,
        @Nullable GridCacheVersion mappedVer,
        GridCacheContext<K, V> ctx
    ) throws IgniteCheckedException {
        int idx = valuesSize();

        dhtVers[idx] = dhtVer;
        mappedVers[idx] = mappedVer;

        if (filterRes != null)
            filterRes[idx] = filterPassed;

        // Delegate to super.
        addValueBytes(val, valBytes, ctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridNearLockResponse _clone = (GridNearLockResponse)_msg;

        _clone.pending = pending;
        _clone.miniId = miniId;
        _clone.dhtVers = dhtVers;
        _clone.mappedVers = mappedVers;
        _clone.filterRes = filterRes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        MessageWriteState state = MessageWriteState.get();
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 11:
                if (!writer.writeObjectArray("dhtVers", dhtVers, GridCacheVersion.class))
                    return false;

                state.increment();

            case 12:
                if (!writer.writeBooleanArray("filterRes", filterRes))
                    return false;

                state.increment();

            case 13:
                if (!writer.writeObjectArray("mappedVers", mappedVers, GridCacheVersion.class))
                    return false;

                state.increment();

            case 14:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                state.increment();

            case 15:
                if (!writer.writeCollection("pending", pending, GridCacheVersion.class))
                    return false;

                state.increment();

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
            case 11:
                dhtVers = reader.readObjectArray("dhtVers", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                filterRes = reader.readBooleanArray("filterRes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                mappedVers = reader.readObjectArray("mappedVers", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 15:
                pending = reader.readCollection("pending", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 52;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockResponse.class, this, super.toString());
    }
}
