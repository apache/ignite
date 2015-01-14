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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
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
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors",
        "OverriddenMethodCallDuringObjectConstruction"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDistributedLockResponse _clone = new GridDistributedLockResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedLockResponse _clone = (GridDistributedLockResponse)_msg;

        _clone.futId = futId;
        _clone.err = err;
        _clone.errBytes = errBytes;
        _clone.valBytes = valBytes;
        _clone.vals = vals;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 8:
                if (!commState.putByteArray(errBytes))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 10:
                if (valBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(valBytes.size()))
                            return false;

                        commState.it = valBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putValueBytes((GridCacheValueBytes)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 8:
                byte[] errBytes0 = commState.getByteArray();

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 9:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 10:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (valBytes == null)
                        valBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheValueBytes _val = commState.getValueBytes();

                        if (_val == VAL_BYTES_NOT_READ)
                            return false;

                        valBytes.add((GridCacheValueBytes)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 23;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockResponse.class, this,
            "valBytesLen", valBytes == null ? 0 : valBytes.size(),
            "super", super.toString());
    }
}
