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
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
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
    private IgnitePredicate<Cache.Entry<K, V>>[] filter;

    /** Filter bytes. */
    private byte[][] filterBytes;

    /** Flag indicating whether request contains primary keys. */
    private boolean hasPrimary;

    /** Force transform backups flag. */
    private boolean forceTransformBackups;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
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
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filter,
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
    @Nullable public IgnitePredicate<Cache.Entry<K, V>>[] filter() {
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeMessage("drExpireTimes", drExpireTimes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("drTtls", drTtls))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("drVers", drVers, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByteArray("expiryPlcBytes", expiryPlcBytes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("fastMap", fastMap))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeObjectArray("filterBytes", filterBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeBoolean("forceTransformBackups", forceTransformBackups))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("hasPrimary", hasPrimary))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection("keyBytes", keyBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeBoolean("retval", retval))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMessage("updateVer", updateVer))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection("valBytes", valBytes, MessageFieldType.MSG))
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
            case 3:
                drExpireTimes = reader.readMessage("drExpireTimes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                drTtls = reader.readMessage("drTtls");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                drVers = reader.readCollection("drVers", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                expiryPlcBytes = reader.readByteArray("expiryPlcBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                fastMap = reader.readBoolean("fastMap");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                filterBytes = reader.readObjectArray("filterBytes", MessageFieldType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                forceTransformBackups = reader.readBoolean("forceTransformBackups");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                hasPrimary = reader.readBoolean("hasPrimary");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageFieldType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                keyBytes = reader.readCollection("keyBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                readState++;

            case 15:
                retval = reader.readBoolean("retval");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 16:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 17:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                readState++;

            case 18:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 19:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 20:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 21:
                valBytes = reader.readCollection("valBytes", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
