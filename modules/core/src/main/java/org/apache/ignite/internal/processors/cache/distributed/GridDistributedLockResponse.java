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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Lock response message.
 */
public class GridDistributedLockResponse<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Value bytes. */
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> valBytes;

    /** Values. */
    @GridToStringInclude
    @GridDirectTransient
    private List<V> vals;

    /**
     * Empty constructor (required by {@link Externalizable}).
     */
    public GridDistributedLockResponse() {
        /* No-op. */
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock version.
     * @param futId Future ID.
     * @param cnt Key count.
     */
    public GridDistributedLockResponse(int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        int cnt) {
        super(lockVer, cnt);

        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;

        vals = new ArrayList<>(cnt);
        valBytes = new ArrayList<>(cnt);
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param err Error.
     */
    public GridDistributedLockResponse(int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        Throwable err) {
        super(lockVer, 0);

        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.err = err;
    }

    /**
     * @param cacheId Cache ID.
     * @param lockVer Lock ID.
     * @param futId Future ID.
     * @param cnt Count.
     * @param err Error.
     */
    public GridDistributedLockResponse(int cacheId,
        GridCacheVersion lockVer,
        IgniteUuid futId,
        int cnt,
        Throwable err) {
        super(lockVer, cnt);

        assert futId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.err = err;

        vals = new ArrayList<>(cnt);
        valBytes = new ArrayList<>(cnt);
    }

    /**
     *
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param err Error to set.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /**
     * @param idx Index of locked flag.
     * @return Value of locked flag at given index.
     */
    public boolean isCurrentlyLocked(int idx) {
        assert idx >= 0;

        Collection<GridCacheMvccCandidate<K>> cands = candidatesByIndex(idx);

        for (GridCacheMvccCandidate<K> cand : cands)
            if (cand.owner())
                return true;

        return false;
    }

    /**
     * @param idx Candidates index.
     * @param cands Collection of candidates.
     * @param committedVers Committed versions relative to lock version.
     * @param rolledbackVers Rolled back versions relative to lock version.
     */
    public void setCandidates(int idx, Collection<GridCacheMvccCandidate<K>> cands,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers) {
        assert idx >= 0;

        completedVersions(committedVers, rolledbackVers);

        candidatesByIndex(idx, cands);
    }

    /**
     * @param idx Value index.
     *
     * @return Value bytes (possibly {@code null}).
     */
    @Nullable public byte[] valueBytes(int idx) {
        if (!F.isEmpty(valBytes)) {
            GridCacheValueBytes res = valBytes.get(idx);

            if (res != null && !res.isPlain())
                return res.get();
        }

        return null;
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes (possibly {@code null}).
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void addValueBytes(V val, @Nullable byte[] valBytes, GridCacheContext<K, V> ctx) throws IgniteCheckedException {
        if (ctx.deploymentEnabled())
            prepareObject(val, ctx.shared());

        GridCacheValueBytes vb = null;

        if (val != null) {
            vb = val instanceof byte[] ? GridCacheValueBytes.plain(val) : valBytes != null ?
                GridCacheValueBytes.marshaled(valBytes) : null;
        }
        else if (valBytes != null)
            vb = GridCacheValueBytes.marshaled(valBytes);

        this.valBytes.add(vb);

        vals.add(val);
    }

    /**
     * @return Values size.
     */
    protected int valuesSize() {
        return vals.size();
    }

    /**
     * @param idx Index.
     * @return Value for given index.
     */
    @Nullable public V value(int idx) {
        if (!F.isEmpty(vals)) {
            V res = vals.get(idx);

            if (res != null)
                return res;
        }

        // If there was no value in values collection, then it could be in value bytes collection in case of byte[].
        if (!F.isEmpty(valBytes)) {
            GridCacheValueBytes res = valBytes.get(idx);

            if (res != null && res.isPlain())
                return (V)res.get();
        }

        // Value is not found in both value and value bytes collections.
        return null;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (F.isEmpty(valBytes) && !F.isEmpty(vals))
            valBytes = marshalValuesCollection(vals, ctx);

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (F.isEmpty(vals) && !F.isEmpty(valBytes))
            vals = unmarshalValueBytesCollection(valBytes, ctx, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), (byte)11))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection("valBytes", valBytes, Type.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 8:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                valBytes = reader.readCollection("valBytes", Type.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 22;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockResponse.class, this,
            "valBytesLen", valBytes == null ? 0 : valBytes.size(),
            "super", super.toString());
    }
}
