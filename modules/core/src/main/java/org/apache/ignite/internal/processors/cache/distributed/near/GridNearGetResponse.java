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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Get response.
 */
public class GridNearGetResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Sub ID. */
    private IgniteUuid miniId;

    /** Version. */
    private GridCacheVersion ver;

    /** Result. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridCacheEntryInfo<K, V>> entries;

    /** */
    private byte[] entriesBytes;

    /** Keys to retry due to ownership shift. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts = new GridLeanSet<>();

    /** Topology version if invalid partitions is not empty. */
    private long topVer;

    /** Error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearGetResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     */
    public GridNearGetResponse(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver
    ) {
        assert futId != null;
        assert miniId != null;
        assert ver != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;
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

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Entries.
     */
    public Collection<GridCacheEntryInfo<K, V>> entries() {
        return entries;
    }

    /**
     * @param entries Entries.
     */
    public void entries(Collection<GridCacheEntryInfo<K, V>> entries) {
        this.entries = entries;
    }

    /**
     * @return Failed filter set.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param invalidParts Partitions to retry due to ownership shift.
     * @param topVer Topology version.
     */
    public void invalidPartitions(Collection<Integer> invalidParts, long topVer) {
        this.invalidParts = invalidParts;
        this.topVer = topVer;
    }

    /**
     * @return Topology version if this response has invalid partitions.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (entries != null) {
            marshalInfos(entries, ctx);

            entriesBytes = ctx.marshaller().marshal(entries);
        }

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (entriesBytes != null) {
            entries = ctx.marshaller().unmarshal(entriesBytes, ldr);

            unmarshalInfos(entries, ctx.cacheContext(cacheId()), ldr);
        }

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearGetResponse _clone = new GridNearGetResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearGetResponse _clone = (GridNearGetResponse)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.ver = ver;
        _clone.entries = entries;
        _clone.entriesBytes = entriesBytes;
        _clone.invalidParts = invalidParts;
        _clone.topVer = topVer;
        _clone.err = err;
        _clone.errBytes = errBytes;
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
            case 3:
                if (!commState.putByteArray(entriesBytes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putByteArray(errBytes))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 6:
                if (invalidParts != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(invalidParts.size()))
                            return false;

                        commState.it = invalidParts.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putInt((int)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 7:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putCacheVersion(ver))
                    return false;

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
            case 3:
                byte[] entriesBytes0 = commState.getByteArray();

                if (entriesBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                entriesBytes = entriesBytes0;

                commState.idx++;

            case 4:
                byte[] errBytes0 = commState.getByteArray();

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 5:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 6:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (invalidParts == null)
                        invalidParts = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (buf.remaining() < 4)
                            return false;

                        int _val = commState.getInt();

                        invalidParts.add((Integer)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 7:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 8:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 9:
                GridCacheVersion ver0 = commState.getCacheVersion();

                if (ver0 == CACHE_VER_NOT_READ)
                    return false;

                ver = ver0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 49;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetResponse.class, this);
    }
}
