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
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Force keys request. This message is sent by node while preloading to force
 * another node to put given keys into the next batch of transmitting entries.
 */
public class GridDhtForceKeysRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini-future ID. */
    private IgniteUuid miniId;

    /** Serialized keys. */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> keyBytes;

    /** Keys to request. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> keys;

    /** Topology version for which keys are requested. */
    private long topVer;

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Mini-future ID.
     * @param keys Keys.
     * @param topVer Topology version.
     */
    GridDhtForceKeysRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        Collection<K> keys,
        long topVer
    ) {
        assert futId != null;
        assert miniId != null;
        assert !F.isEmpty(keys);

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.keys = keys;
        this.topVer = topVer;
    }

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtForceKeysRequest() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @param keys Collection of keys.
     */
    public GridDhtForceKeysRequest(Collection<K> keys) {
        assert !F.isEmpty(keys);

        this.keys = keys;
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
     * @return Collection of serialized keys.
     */
    public Collection<byte[]> keyBytes() {
        return keyBytes;
    }

    /**
     * @return Keys.
     */
    public Collection<K> keys() {
        return keys;
    }

    /**
     * @return Topology version for which keys are requested.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (keyBytes == null)
            keyBytes = marshalCollection(keys, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null)
            keys = unmarshalCollection(keyBytes, ctx, ldr);
    }

    /**
     * @return Key count.
     */
    private int keyCount() {
        return keyBytes == null ? keys.size() : keyBytes.size();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtForceKeysRequest _clone = new GridDhtForceKeysRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtForceKeysRequest _clone = (GridDhtForceKeysRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.keyBytes = keyBytes;
        _clone.keys = keys;
        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 3:
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 4:
                if (keyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, keyBytes.size()))
                            return false;

                        commState.it = keyBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray(null, (byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 5:
                if (!commState.putGridUuid("miniId", miniId))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putLong("topVer", topVer))
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
                futId = commState.getGridUuid("futId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 4:
                if (commState.readSize == -1) {
                    commState.readSize = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                }

                if (commState.readSize >= 0) {
                    if (keyBytes == null)
                        keyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray(null);

                        if (!commState.lastRead())
                            return false;

                        keyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 5:
                miniId = commState.getGridUuid("miniId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 6:
                topVer = commState.getLong("topVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 41;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtForceKeysRequest.class, this, "keyCnt", keyCount(), "super", super.toString());
    }
}
