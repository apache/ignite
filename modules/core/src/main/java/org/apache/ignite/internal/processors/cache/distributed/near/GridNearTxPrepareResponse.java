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
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near cache prepare response.
 */
public class GridNearTxPrepareResponse<K, V> extends GridDistributedTxPrepareResponse<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of versions that are pending and less than lock version. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pending;

    /** Future ID.  */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** DHT version. */
    private GridCacheVersion dhtVer;

    /** */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts;

    /** Map of owned values to set on near node. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> ownedVals;

    /** Marshalled owned bytes. */
    @GridToStringExclude
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> ownedValsBytes;

    /** Cache return value. */
    @GridDirectTransient
    private GridCacheReturn<V> retVal;

    /** Return value bytes. */
    private byte[] retValBytes;

    /** Filter failed keys. */
    @GridDirectTransient
    private Collection<IgniteTxKey<K>> filterFailedKeys;

    /** Filter failed key bytes. */
    private byte[] filterFailedKeyBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearTxPrepareResponse() {
        // No-op.
    }

    /**
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param dhtVer DHT version.
     * @param invalidParts Invalid partitions.
     * @param err Error.
     */
    public GridNearTxPrepareResponse(
        GridCacheVersion xid,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion dhtVer,
        Collection<Integer> invalidParts,
        GridCacheReturn<V> retVal,
        Throwable err
    ) {
        super(xid, err);

        assert futId != null;
        assert miniId != null;
        assert dhtVer != null;

        this.futId = futId;
        this.miniId = miniId;
        this.dhtVer = dhtVer;
        this.invalidParts = invalidParts;
        this.retVal = retVal;
    }

    /**
     * Gets pending versions that are less than {@link #version()}.
     *
     * @return Pending versions.
     */
    public Collection<GridCacheVersion> pending() {
        return pending == null ? Collections.<GridCacheVersion>emptyList() : pending;
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
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * Adds owned value.
     *
     * @param key Key.
     * @param ver DHT version.
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public void addOwnedValue(IgniteTxKey<K> key, GridCacheVersion ver, V val, byte[] valBytes) {
        if (val == null && valBytes == null)
            return;

        if (ownedVals == null)
            ownedVals = new HashMap<>();

        ownedVals.put(key, F.t(ver, val, valBytes));
    }

    /**
     * @return Owned values map.
     */
    public Map<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> ownedValues() {
        return ownedVals == null ? Collections.<IgniteTxKey<K>, GridTuple3<GridCacheVersion,V,byte[]>>emptyMap() :
            Collections.unmodifiableMap(ownedVals);
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn<V> returnValue() {
        return retVal;
    }

    /**
     * @param filterFailedKeys Collection of keys that did not pass the filter.
     */
    public void filterFailedKeys(Collection<IgniteTxKey<K>> filterFailedKeys) {
        this.filterFailedKeys = filterFailedKeys;
    }

    /**
     * @return Collection of keys that did not pass the filter.
     */
    public Collection<IgniteTxKey<K>> filterFailedKeys() {
        return filterFailedKeys == null ? Collections.<IgniteTxKey<K>>emptyList() : filterFailedKeys;
    }

    /**
     * @param key Key.
     * @return {@code True} if response has owned value for given key.
     */
    public boolean hasOwnedValue(IgniteTxKey<K> key) {
        return ownedVals != null && ownedVals.containsKey(key);
    }

    /**
     * @return Invalid partitions.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (ownedVals != null && ownedValsBytes == null) {
            ownedValsBytes = new ArrayList<>(ownedVals.size());

            for (Map.Entry<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> entry : ownedVals.entrySet()) {
                GridTuple3<GridCacheVersion, V, byte[]> tup = entry.getValue();

                boolean rawBytes = false;

                byte[] valBytes = tup.get3();

                if (valBytes == null) {
                    if (tup.get2() != null && tup.get2() instanceof byte[]) {
                        rawBytes = true;

                        valBytes = (byte[])tup.get2();
                    }
                    else
                        valBytes = ctx.marshaller().marshal(tup.get2());
                }

                ownedValsBytes.add(ctx.marshaller().marshal(F.t(entry.getKey(), tup.get1(), valBytes, rawBytes)));
            }
        }


        if (retValBytes == null && retVal != null)
            retValBytes = ctx.marshaller().marshal(retVal);

        if (filterFailedKeyBytes == null && filterFailedKeys != null)
            filterFailedKeyBytes = ctx.marshaller().marshal(filterFailedKeys);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedValsBytes != null && ownedVals == null) {
            ownedVals = new HashMap<>();

            for (byte[] bytes : ownedValsBytes) {
                GridTuple4<IgniteTxKey<K>, GridCacheVersion, byte[], Boolean> tup = ctx.marshaller().unmarshal(bytes, ldr);

                V val = tup.get4() ? (V)tup.get3() : ctx.marshaller().<V>unmarshal(tup.get3(), ldr);

                ownedVals.put(tup.get1(), F.t(tup.get2(), val, tup.get4() ? null : tup.get3()));
            }
        }

        if (retVal == null && retValBytes != null)
            retVal = ctx.marshaller().unmarshal(retValBytes, ldr);

        if (filterFailedKeys == null && filterFailedKeyBytes != null)
            filterFailedKeys = ctx.marshaller().unmarshal(filterFailedKeyBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearTxPrepareResponse _clone = new GridNearTxPrepareResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearTxPrepareResponse _clone = (GridNearTxPrepareResponse)_msg;

        _clone.pending = pending;
        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.dhtVer = dhtVer;
        _clone.invalidParts = invalidParts;
        _clone.ownedVals = ownedVals;
        _clone.ownedValsBytes = ownedValsBytes;
        _clone.retVal = retVal;
        _clone.retValBytes = retValBytes;
        _clone.filterFailedKeys = filterFailedKeys;
        _clone.filterFailedKeyBytes = filterFailedKeyBytes;
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
            case 10:
                if (!commState.putCacheVersion(dhtVer))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putByteArray(filterFailedKeyBytes))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 13:
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

            case 14:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 15:
                if (ownedValsBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(ownedValsBytes.size()))
                            return false;

                        commState.it = ownedValsBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 16:
                if (pending != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(pending.size()))
                            return false;

                        commState.it = pending.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putCacheVersion((GridCacheVersion)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 17:
                if (!commState.putByteArray(retValBytes))
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
            case 10:
                GridCacheVersion dhtVer0 = commState.getCacheVersion();

                if (dhtVer0 == CACHE_VER_NOT_READ)
                    return false;

                dhtVer = dhtVer0;

                commState.idx++;

            case 11:
                byte[] filterFailedKeyBytes0 = commState.getByteArray();

                if (filterFailedKeyBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                filterFailedKeyBytes = filterFailedKeyBytes0;

                commState.idx++;

            case 12:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 13:
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

            case 14:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 15:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (ownedValsBytes == null)
                        ownedValsBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        ownedValsBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 16:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (pending == null)
                        pending = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        pending.add((GridCacheVersion)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 17:
                byte[] retValBytes0 = commState.getByteArray();

                if (retValBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                retValBytes = retValBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 55;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareResponse.class, this, "super", super.toString());
    }
}
