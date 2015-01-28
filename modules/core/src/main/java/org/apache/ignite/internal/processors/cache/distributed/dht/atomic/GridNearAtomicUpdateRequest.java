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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicUpdateRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Target node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Fast map flag. */
    private boolean fastMap;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    private long topVer;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    private GridCacheOperation op;

    /** Keys to update. */
    @GridDirectTransient
    @GridToStringInclude
    private List<K> keys;

    /** Key bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Values to update. */
    @GridDirectTransient
    private List<Object> vals;

    /** Value bytes. */
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> valBytes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** DR versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> drVers;

    /** DR TTLs. */
    private GridLongList drTtls;

    /** DR TTLs. */
    private GridLongList drExpireTimes;

    /** Return value flag. */
    private boolean retval;

    /** Expiry policy. */
    @GridDirectTransient
    private ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    private byte[] expiryPlcBytes;

    /** Filter. */
    @GridDirectTransient
    private IgnitePredicate<CacheEntry<K, V>>[] filter;

    /** Filter bytes. */
    private byte[][] filterBytes;

    /** Flag indicating whether request contains primary keys. */
    private boolean hasPrimary;

    /** Force transform backups flag. */
    @GridDirectVersion(2)
    private boolean forceTransformBackups;

    /** Subject ID. */
    @GridDirectVersion(3)
    private UUID subjId;

    /** Task name hash. */
    @GridDirectVersion(4)
    private int taskNameHash;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param fastMap Fast map scheme flag.
     * @param updateVer Update version set if fast map is performed.
     * @param topVer Topology version.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param forceTransformBackups Force transform backups flag.
     * @param expiryPlc Expiry policy.
     * @param invokeArgs Optional arguments for entry processor.
     * @param filter Optional filter for atomic check.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridNearAtomicUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        long topVer,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        boolean forceTransformBackups,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Object[] invokeArgs,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.fastMap = fastMap;
        this.updateVer = updateVer;

        this.topVer = topVer;
        this.syncMode = syncMode;
        this.op = op;
        this.retval = retval;
        this.forceTransformBackups = forceTransformBackups;
        this.expiryPlc = expiryPlc;
        this.invokeArgs = invokeArgs;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        keys = new ArrayList<>();
        vals = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Mapped node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    public boolean fastMap() {
        return fastMap;
    }

    /**
     * @return Update version for fast-map request.
     */
    public GridCacheVersion updateVersion() {
        return updateVer;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Expiry policy.
     */
    public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * @return Return value flag.
     */
    public boolean returnValue() {
        return retval;
    }

    /**
     * @return Filter.
     */
    @Nullable public IgnitePredicate<CacheEntry<K, V>>[] filter() {
        return filter;
    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param drTtl DR TTL (optional).
     * @param drExpireTime DR expire time (optional).
     * @param drVer DR version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public void addUpdateEntry(K key,
        @Nullable Object val,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer,
        boolean primary) {
        assert val != null || op == DELETE;
        assert op != TRANSFORM || val instanceof EntryProcessor;

        keys.add(key);
        vals.add(val);

        hasPrimary |= primary;

        // In case there is no DR, do not create the list.
        if (drVer != null) {
            if (drVers == null) {
                drVers = new ArrayList<>();

                for (int i = 0; i < keys.size() - 1; i++)
                    drVers.add(null);
            }

            drVers.add(drVer);
        }
        else if (drVers != null)
            drVers.add(drVer);

        if (drTtl >= 0) {
            if (drTtls == null) {
                drTtls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    drTtls.add(-1);
            }

            drTtls.add(drTtl);
        }

        if (drExpireTime >= 0) {
            if (drExpireTimes == null) {
                drExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    drExpireTimes.add(-1);
            }

            drExpireTimes.add(drExpireTime);
        }
    }

    /**
     * @return Keys for this update request.
     */
    public List<K> keys() {
        return keys;
    }

    /**
     * @return Values for this update request.
     */
    public List<Object> values() {
        return vals;
    }

    /**
     * @return Update operation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    public V value(int idx) {
        assert op == UPDATE : op;

        return (V)vals.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    public EntryProcessor<K, V, ?> entryProcessor(int idx) {
        assert op == TRANSFORM : op;

        return (EntryProcessor<K, V, ?>)vals.get(idx);
    }

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public Object writeValue(int idx) {
        if (vals != null) {
            Object val = vals.get(idx);

            if (val != null)
                return val;
        }

        if (valBytes != null) {
            GridCacheValueBytes valBytesTuple = valBytes.get(idx);

            if (valBytesTuple != null && valBytesTuple.isPlain())
                return valBytesTuple.get();
        }

        return null;
    }

    /**
     * @param idx Key index.
     * @return Value bytes.
     */
    public byte[] valueBytes(int idx) {
        if (op != TRANSFORM && valBytes != null) {
            GridCacheValueBytes valBytesTuple = valBytes.get(idx);

            if (valBytesTuple != null && !valBytesTuple.isPlain())
                return valBytesTuple.get();
        }

        return null;
    }

    /**
     * @return DR versions.
     */
    @Nullable public List<GridCacheVersion> drVersions() {
        return drVers;
    }

    /**
     * @param idx Index.
     * @return DR version.
     */
    @Nullable public GridCacheVersion drVersion(int idx) {
        if (drVers != null) {
            assert idx >= 0 && idx < drVers.size();

            return drVers.get(idx);
        }

        return null;
    }

    /**
     * @return DR TTLs.
     */
    @Nullable public GridLongList drTtls() {
        return drTtls;
    }

    /**
     * @param idx Index.
     * @return DR TTL.
     */
    public long drTtl(int idx) {
        if (drTtls != null) {
            assert idx >= 0 && idx < drTtls.size();

            return drTtls.get(idx);
        }

        return -1L;
    }

    /**
     * @return DR TTLs.
     */
    @Nullable public GridLongList drExpireTimes() {
        return drExpireTimes;
    }

    /**
     * @param idx Index.
     * @return DR TTL.
     */
    public long drExpireTime(int idx) {
        if (drExpireTimes != null) {
            assert idx >= 0 && idx < drExpireTimes.size();

            return drExpireTimes.get(idx);
        }

        return -1L;
    }

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    public boolean hasPrimary() {
        return hasPrimary;
    }

    /**
     * @return Force transform backups flag.
     */
    public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /**
     * @param forceTransformBackups Force transform backups flag.
     */
    public void forceTransformBackups(boolean forceTransformBackups) {
        this.forceTransformBackups = forceTransformBackups;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        keyBytes = marshalCollection(keys, ctx);
        valBytes = marshalValuesCollection(vals, ctx);
        filterBytes = marshalFilter(filter, ctx);
        invokeArgsBytes = marshalInvokeArguments(invokeArgs, ctx);

        if (expiryPlc != null)
            expiryPlcBytes = CU.marshal(ctx, new IgniteExternalizableExpiryPolicy(expiryPlc));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        keys = unmarshalCollection(keyBytes, ctx, ldr);
        vals = unmarshalValueBytesCollection(valBytes, ctx, ldr);
        filter = unmarshalFilter(filterBytes, ctx, ldr);
        invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);

        if (expiryPlcBytes != null)
            expiryPlc = ctx.marshaller().unmarshal(expiryPlcBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearAtomicUpdateRequest _clone = new GridNearAtomicUpdateRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearAtomicUpdateRequest _clone = (GridNearAtomicUpdateRequest)_msg;

        _clone.nodeId = nodeId;
        _clone.futVer = futVer;
        _clone.fastMap = fastMap;
        _clone.updateVer = updateVer;
        _clone.topVer = topVer;
        _clone.syncMode = syncMode;
        _clone.op = op;
        _clone.keys = keys;
        _clone.keyBytes = keyBytes;
        _clone.vals = vals;
        _clone.valBytes = valBytes;
        _clone.invokeArgs = invokeArgs;
        _clone.invokeArgsBytes = invokeArgsBytes;
        _clone.drVers = drVers;
        _clone.drTtls = drTtls;
        _clone.drExpireTimes = drExpireTimes;
        _clone.retval = retval;
        _clone.expiryPlc = expiryPlc;
        _clone.expiryPlcBytes = expiryPlcBytes;
        _clone.filter = filter;
        _clone.filterBytes = filterBytes;
        _clone.hasPrimary = hasPrimary;
        _clone.forceTransformBackups = forceTransformBackups;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
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
                if (!commState.putLongList(drExpireTimes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLongList(drTtls))
                    return false;

                commState.idx++;

            case 5:
                if (drVers != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(drVers.size()))
                            return false;

                        commState.it = drVers.iterator();
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

            case 6:
                if (!commState.putByteArray(expiryPlcBytes))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putBoolean(fastMap))
                    return false;

                commState.idx++;

            case 8:
                if (filterBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(filterBytes.length))
                            return false;

                        commState.it = arrayIterator(filterBytes);
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

            case 9:
                if (!commState.putCacheVersion(futVer))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putBoolean(hasPrimary))
                    return false;

                commState.idx++;

            case 11:
                if (invokeArgsBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(invokeArgsBytes.length))
                            return false;

                        commState.it = arrayIterator(invokeArgsBytes);
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

            case 12:
                if (keyBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(keyBytes.size()))
                            return false;

                        commState.it = keyBytes.iterator();
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

            case 13:
                if (!commState.putEnum(op))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putBoolean(retval))
                    return false;

                commState.idx++;

            case 15:
                if (!commState.putEnum(syncMode))
                    return false;

                commState.idx++;

            case 16:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putCacheVersion(updateVer))
                    return false;

                commState.idx++;

            case 18:
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

            case 19:
                if (!commState.putBoolean(forceTransformBackups))
                    return false;

                commState.idx++;

            case 20:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

            case 21:
                if (!commState.putInt(taskNameHash))
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
                GridLongList drExpireTimes0 = commState.getLongList();

                if (drExpireTimes0 == LONG_LIST_NOT_READ)
                    return false;

                drExpireTimes = drExpireTimes0;

                commState.idx++;

            case 4:
                GridLongList drTtls0 = commState.getLongList();

                if (drTtls0 == LONG_LIST_NOT_READ)
                    return false;

                drTtls = drTtls0;

                commState.idx++;

            case 5:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (drVers == null)
                        drVers = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridCacheVersion _val = commState.getCacheVersion();

                        if (_val == CACHE_VER_NOT_READ)
                            return false;

                        drVers.add((GridCacheVersion)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 6:
                byte[] expiryPlcBytes0 = commState.getByteArray();

                if (expiryPlcBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                expiryPlcBytes = expiryPlcBytes0;

                commState.idx++;

            case 7:
                if (buf.remaining() < 1)
                    return false;

                fastMap = commState.getBoolean();

                commState.idx++;

            case 8:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (filterBytes == null)
                        filterBytes = new byte[commState.readSize][];

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        filterBytes[i] = (byte[])_val;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 9:
                GridCacheVersion futVer0 = commState.getCacheVersion();

                if (futVer0 == CACHE_VER_NOT_READ)
                    return false;

                futVer = futVer0;

                commState.idx++;

            case 10:
                if (buf.remaining() < 1)
                    return false;

                hasPrimary = commState.getBoolean();

                commState.idx++;

            case 11:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (invokeArgsBytes == null)
                        invokeArgsBytes = new byte[commState.readSize][];

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        invokeArgsBytes[i] = (byte[])_val;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 12:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (keyBytes == null)
                        keyBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        keyBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 13:
                if (buf.remaining() < 1)
                    return false;

                byte op0 = commState.getByte();

                op = GridCacheOperation.fromOrdinal(op0);

                commState.idx++;

            case 14:
                if (buf.remaining() < 1)
                    return false;

                retval = commState.getBoolean();

                commState.idx++;

            case 15:
                if (buf.remaining() < 1)
                    return false;

                byte syncMode0 = commState.getByte();

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncMode0);

                commState.idx++;

            case 16:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

            case 17:
                GridCacheVersion updateVer0 = commState.getCacheVersion();

                if (updateVer0 == CACHE_VER_NOT_READ)
                    return false;

                updateVer = updateVer0;

                commState.idx++;

            case 18:
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

            case 19:
                if (buf.remaining() < 1)
                    return false;

                forceTransformBackups = commState.getBoolean();

                commState.idx++;

            case 20:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

            case 21:
                if (buf.remaining() < 4)
                    return false;

                taskNameHash = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 39;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
