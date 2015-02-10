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
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Lite dht cache backup update request.
 */
public class GridDhtAtomicUpdateRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID. */
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Write version. */
    private GridCacheVersion writeVer;

    /** Topology version. */
    private long topVer;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> keys;

    /** Key bytes. */
    @GridToStringInclude
    @GridDirectCollection(byte[].class)
    private List<byte[]> keyBytes;

    /** Values to update. */
    @GridToStringInclude
    @GridDirectTransient
    private List<V> vals;

    /** Value bytes. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> valBytes;

    /** DR versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> drVers;

    /** TTLs. */
    private GridLongList ttls;

    /** DR expire time. */
    private GridLongList drExpireTimes;

    /** Near TTLs. */
    private GridLongList nearTtls;

    /** Near expire times. */
    private GridLongList nearExpireTimes;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> nearKeys;

    /** Key bytes. */
    @GridToStringInclude
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearKeyBytes;

    /** Values to update. */
    @GridToStringInclude
    @GridDirectTransient
    private List<V> nearVals;

    /** Value bytes. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheValueBytes.class)
    private List<GridCacheValueBytes> nearValBytes;

    /** Force transform backups flag. */
    private boolean forceTransformBackups;

    /** Entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<K, V, ?>> entryProcessors;

    /** Entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> entryProcessorsBytes;

    /** Near entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<K, V, ?>> nearEntryProcessors;

    /** Near entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearEntryProcessorsBytes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param writeVer Write version for cache values.
     * @param invokeArgs Optional arguments for entry processor.
     * @param syncMode Cache write synchronization mode.
     * @param topVer Topology version.
     * @param forceTransformBackups Force transform backups flag.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     */
    public GridDhtAtomicUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        long topVer,
        boolean forceTransformBackups,
        UUID subjId,
        int taskNameHash,
        Object[] invokeArgs
    ) {
        assert invokeArgs == null || forceTransformBackups;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.writeVer = writeVer;
        this.syncMode = syncMode;
        this.topVer = topVer;
        this.forceTransformBackups = forceTransformBackups;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.invokeArgs = invokeArgs;

        keys = new ArrayList<>();
        keyBytes = new ArrayList<>();

        if (forceTransformBackups) {
            entryProcessors = new ArrayList<>();
            entryProcessorsBytes = new ArrayList<>();
        }
        else {
            vals = new ArrayList<>();
            valBytes = new ArrayList<>();
        }
    }

    /**
     * @return Force transform backups flag.
     */
    public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /**
     * @param key Key to add.
     * @param keyBytes Key bytes, if key was already serialized.
     * @param val Value, {@code null} if should be removed.
     * @param valBytes Value bytes, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param drExpireTime DR expire time (optional).
     * @param drVer DR version (optional).
     */
    public void addWriteValue(K key,
        @Nullable byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        EntryProcessor<K, V, ?> entryProcessor,
        long ttl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer) {
        keys.add(key);
        this.keyBytes.add(keyBytes);

        if (forceTransformBackups) {
            assert entryProcessor != null;

            entryProcessors.add(entryProcessor);
        }
        else {
            vals.add(val);
            this.valBytes.add(valBytes != null ? GridCacheValueBytes.marshaled(valBytes) : null);
        }

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

        if (ttl >= 0) {
            if (ttls == null) {
                ttls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    ttls.add(-1);
            }
        }

        if (ttls != null)
            ttls.add(ttl);

        if (drExpireTime >= 0) {
            if (drExpireTimes == null) {
                drExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    drExpireTimes.add(-1);
            }
        }

        if (drExpireTimes != null)
            drExpireTimes.add(drExpireTime);
    }

    /**
     * @param key Key to add.
     * @param keyBytes Key bytes, if key was already serialized.
     * @param val Value, {@code null} if should be removed.
     * @param valBytes Value bytes, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public void addNearWriteValue(K key,
        @Nullable byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        EntryProcessor<K, V, ?> entryProcessor,
        long ttl,
        long expireTime)
    {
        if (nearKeys == null) {
            nearKeys = new ArrayList<>();
            nearKeyBytes = new ArrayList<>();

            if (forceTransformBackups) {
                nearEntryProcessors = new ArrayList<>();
                nearEntryProcessorsBytes = new ArrayList<>();
            }
            else {
                nearVals = new ArrayList<>();
                nearValBytes = new ArrayList<>();
            }
        }

        nearKeys.add(key);
        nearKeyBytes.add(keyBytes);

        if (forceTransformBackups) {
            assert entryProcessor != null;

            nearEntryProcessors.add(entryProcessor);
        }
        else {
            nearVals.add(val);
            nearValBytes.add(valBytes != null ? GridCacheValueBytes.marshaled(valBytes) : null);
        }

        if (ttl >= 0) {
            if (nearTtls == null) {
                nearTtls = new GridLongList(nearKeys.size());

                for (int i = 0; i < nearKeys.size() - 1; i++)
                    nearTtls.add(-1);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);

        if (expireTime >= 0) {
            if (nearExpireTimes == null) {
                nearExpireTimes = new GridLongList(nearKeys.size());

                for (int i = 0; i < nearKeys.size() - 1; i++)
                    nearExpireTimes.add(-1);
            }
        }

        if (nearExpireTimes != null)
            nearExpireTimes.add(expireTime);
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Keys size.
     */
    public int size() {
        return keys.size();
    }

    /**
     * @return Keys size.
     */
    public int nearSize() {
        return nearKeys != null ? nearKeys.size() : 0;
    }

    /**
     * @return Version assigned on primary node.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Keys.
     */
    public Collection<K> keys() {
        return keys;
    }

    /**
     * @param idx Key index.
     * @return Key.
     */
    public K key(int idx) {
        return keys.get(idx);
    }

    /**
     * @param idx Near key index.
     * @return Key.
     */
    public K nearKey(int idx) {
        return nearKeys.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Key bytes.
     */
    @Nullable public byte[] keyBytes(int idx) {
        return keyBytes == null ? null : keyBytes.get(idx);
    }

    /**
     * @param idx Near key index.
     * @return Key bytes.
     */
    @Nullable public byte[] nearKeyBytes(int idx) {
        return nearKeyBytes == null ? null : nearKeyBytes.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public V value(int idx) {
        if (vals != null) {
            V val = vals.get(idx);

            if (val != null)
                return val;
        }

        if (valBytes != null) {
            GridCacheValueBytes valBytes0 = valBytes.get(idx);

            if (valBytes0 != null && valBytes0.isPlain())
                return (V)valBytes0.get();
        }

        return null;
    }

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Nullable public EntryProcessor<K, V, ?> entryProcessor(int idx) {
        return entryProcessors == null ? null : entryProcessors.get(idx);
    }

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Nullable public V nearValue(int idx) {
        if (nearVals != null) {
            V val = nearVals.get(idx);

            if (val != null)
                return val;
        }

        if (nearValBytes != null) {
            GridCacheValueBytes valBytes0 = nearValBytes.get(idx);

            if (valBytes0 != null && valBytes0.isPlain())
                return (V)valBytes0.get();
        }

        return null;
    }

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Nullable public EntryProcessor<K, V, ?> nearEntryProcessor(int idx) {
        return nearEntryProcessors == null ? null : nearEntryProcessors.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Value bytes.
     */
    @Nullable public byte[] valueBytes(int idx) {
        if (valBytes != null) {
            GridCacheValueBytes valBytes0 = valBytes.get(idx);

            if (valBytes0 != null && !valBytes0.isPlain())
                return valBytes0.get();
        }

        return null;
    }

    /**
     * @param idx Near key index.
     * @return Value bytes.
     */
    @Nullable public byte[] nearValueBytes(int idx) {
        if (nearValBytes != null) {
            GridCacheValueBytes valBytes0 = nearValBytes.get(idx);

            if (valBytes0 != null && !valBytes0.isPlain())
                return valBytes0.get();
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
     * @param idx Index.
     * @return TTL.
     */
    public long ttl(int idx) {
        if (ttls != null) {
            assert idx >= 0 && idx < ttls.size();

            return ttls.get(idx);
        }

        return -1L;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public long nearTtl(int idx) {
        if (nearTtls != null) {
            assert idx >= 0 && idx < nearTtls.size();

            return nearTtls.get(idx);
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
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public long nearExpireTime(int idx) {
        if (nearExpireTimes != null) {
            assert idx >= 0 && idx < nearExpireTimes.size();

            return nearExpireTimes.get(idx);
        }

        return -1L;
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        keyBytes = marshalCollection(keys, ctx);
        valBytes = marshalValuesCollection(vals, ctx);

        if (forceTransformBackups) {
            invokeArgsBytes = marshalInvokeArguments(invokeArgs, ctx);

            entryProcessorsBytes = marshalCollection(entryProcessors, ctx);
        }

        nearKeyBytes = marshalCollection(nearKeys, ctx);
        nearValBytes = marshalValuesCollection(nearVals, ctx);

        if (forceTransformBackups)
            nearEntryProcessorsBytes = marshalCollection(nearEntryProcessors, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        keys = unmarshalCollection(keyBytes, ctx, ldr);
        vals = unmarshalValueBytesCollection(valBytes, ctx, ldr);

        if (forceTransformBackups) {
            entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);

            invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }

        nearKeys = unmarshalCollection(nearKeyBytes, ctx, ldr);
        nearVals = unmarshalValueBytesCollection(nearValBytes, ctx, ldr);

        if (forceTransformBackups)
            nearEntryProcessors = unmarshalCollection(nearEntryProcessorsBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridDhtAtomicUpdateRequest _clone = new GridDhtAtomicUpdateRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAtomicUpdateRequest _clone = (GridDhtAtomicUpdateRequest)_msg;

        _clone.nodeId = nodeId;
        _clone.futVer = futVer != null ? (GridCacheVersion)futVer.clone() : null;
        _clone.writeVer = writeVer != null ? (GridCacheVersion)writeVer.clone() : null;
        _clone.topVer = topVer;
        _clone.keys = keys;
        _clone.keyBytes = keyBytes;
        _clone.vals = vals;
        _clone.valBytes = valBytes;
        _clone.drVers = drVers;
        _clone.ttls = ttls != null ? (GridLongList)ttls.clone() : null;
        _clone.drExpireTimes = drExpireTimes != null ? (GridLongList)drExpireTimes.clone() : null;
        _clone.nearTtls = nearTtls != null ? (GridLongList)nearTtls.clone() : null;
        _clone.nearExpireTimes = nearExpireTimes != null ? (GridLongList)nearExpireTimes.clone() : null;
        _clone.syncMode = syncMode;
        _clone.nearKeys = nearKeys;
        _clone.nearKeyBytes = nearKeyBytes;
        _clone.nearVals = nearVals;
        _clone.nearValBytes = nearValBytes;
        _clone.forceTransformBackups = forceTransformBackups;
        _clone.entryProcessors = entryProcessors;
        _clone.entryProcessorsBytes = entryProcessorsBytes;
        _clone.nearEntryProcessors = nearEntryProcessors;
        _clone.nearEntryProcessorsBytes = nearEntryProcessorsBytes;
        _clone.invokeArgs = invokeArgs;
        _clone.invokeArgsBytes = invokeArgsBytes;
        _clone.subjId = subjId;
        _clone.taskNameHash = taskNameHash;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 3:
                if (!writer.writeMessage("drExpireTimes", drExpireTimes))
                    return false;

                state++;

            case 4:
                if (!writer.writeCollection("drVers", drVers, GridCacheVersion.class))
                    return false;

                state++;

            case 5:
                if (!writer.writeCollection("entryProcessorsBytes", entryProcessorsBytes, byte[].class))
                    return false;

                state++;

            case 6:
                if (!writer.writeBoolean("forceTransformBackups", forceTransformBackups))
                    return false;

                state++;

            case 7:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                state++;

            case 8:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, byte[].class))
                    return false;

                state++;

            case 9:
                if (!writer.writeCollection("keyBytes", keyBytes, byte[].class))
                    return false;

                state++;

            case 10:
                if (!writer.writeCollection("nearEntryProcessorsBytes", nearEntryProcessorsBytes, byte[].class))
                    return false;

                state++;

            case 11:
                if (!writer.writeMessage("nearExpireTimes", nearExpireTimes))
                    return false;

                state++;

            case 12:
                if (!writer.writeCollection("nearKeyBytes", nearKeyBytes, byte[].class))
                    return false;

                state++;

            case 13:
                if (!writer.writeMessage("nearTtls", nearTtls))
                    return false;

                state++;

            case 14:
                if (!writer.writeCollection("nearValBytes", nearValBytes, GridCacheValueBytes.class))
                    return false;

                state++;

            case 15:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                state++;

            case 16:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                state++;

            case 17:
                if (!writer.writeEnum("syncMode", syncMode))
                    return false;

                state++;

            case 18:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                state++;

            case 19:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state++;

            case 20:
                if (!writer.writeMessage("ttls", ttls))
                    return false;

                state++;

            case 21:
                if (!writer.writeCollection("valBytes", valBytes, GridCacheValueBytes.class))
                    return false;

                state++;

            case 22:
                if (!writer.writeMessage("writeVer", writeVer))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 3:
                drExpireTimes = reader.readMessage("drExpireTimes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 4:
                drVers = reader.readCollection("drVers", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 5:
                entryProcessorsBytes = reader.readCollection("entryProcessorsBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 6:
                forceTransformBackups = reader.readBoolean("forceTransformBackups");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 7:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 8:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 9:
                keyBytes = reader.readCollection("keyBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 10:
                nearEntryProcessorsBytes = reader.readCollection("nearEntryProcessorsBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 11:
                nearExpireTimes = reader.readMessage("nearExpireTimes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 12:
                nearKeyBytes = reader.readCollection("nearKeyBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 13:
                nearTtls = reader.readMessage("nearTtls");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 14:
                nearValBytes = reader.readCollection("nearValBytes", GridCacheValueBytes.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 15:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 16:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 17:
                syncMode = reader.readEnum("syncMode", CacheWriteSynchronizationMode.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 18:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 19:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 20:
                ttls = reader.readMessage("ttls");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 21:
                valBytes = reader.readCollection("valBytes", GridCacheValueBytes.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 22:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 38;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateRequest.class, this);
    }
}
