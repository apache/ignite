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

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicUpdateRequest extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    public static final ThreadLocal<Set<T2<Long, UUID>>> FUT_VERS = new ThreadLocal<Set<T2<Long, UUID>>>() {
        @Override protected Set<T2<Long, UUID>> initialValue() {
            return new HashSet<>();
        }
    };

    public static final ThreadLocal<Long> RECEIVED_TIMESTAMP = new ThreadLocal<Long>() {
        @Override protected Long initialValue() {
            return 0L;
        }
    };

    public static final ConcurrentMap<String, Long> PREPARED = new ConcurrentHashMap8<>();
    public static final ConcurrentMap<String, Long> SENT = new ConcurrentHashMap8<>();
    public static final ConcurrentMap<String, Long> RECEIVED = new ConcurrentHashMap8<>();
    public static final ConcurrentMap<String, Long> HANDLED = new ConcurrentHashMap8<>();
    public static final ConcurrentMap<String, Long> SUBMITTED = new ConcurrentHashMap8<>();
    public static final ConcurrentMap<String, Long> PROC_STARTED = new ConcurrentHashMap8<>();

    public static final int SAMPLE_MOD = 100;

    /** Target node ID. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private long futVer;

    /** Fast map flag. */
    private boolean fastMap;

    /** Update version. Set to non-null if fastMap is {@code true}. */
    private GridCacheVersion updateVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    private boolean topLocked;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    private GridCacheOperation op;

    /** Keys to update. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Values to update. */
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> vals;

//    /** Entry processors. */
//    @GridDirectTransient
//    private List<EntryProcessor<Object, Object, Object>> entryProcessors;
//
//    /** Entry processors bytes. */
//    @GridDirectCollection(byte[].class)
//    private List<byte[]> entryProcessorsBytes;
//
//    /** Optional arguments for entry processor. */
//    @GridDirectTransient
//    private Object[] invokeArgs;
//
//    /** Entry processor arguments bytes. */
//    private byte[][] invokeArgsBytes;

//    /** Conflict versions. */
//    @GridDirectCollection(GridCacheVersion.class)
//    private List<GridCacheVersion> conflictVers;
//
//    /** Conflict TTLs. */
//    private GridLongList conflictTtls;
//
//    /** Conflict expire times. */
//    private GridLongList conflictExpireTimes;

    /** Return value flag. */
    private boolean retval;

//    /** Expiry policy. */
//    @GridDirectTransient
//    private ExpiryPolicy expiryPlc;
//
//    /** Expiry policy bytes. */
//    private byte[] expiryPlcBytes;

//    /** Filter. */
//    private CacheEntryPredicate[] filter;

    /** Flag indicating whether request contains primary keys. */
    private boolean hasPrimary;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Skip write-through to a persistent storage. */
    private boolean skipStore;

    /** */
    private boolean clientReq;

    /** Keep binary flag. */
    private boolean keepBinary;

    /** */
    @GridDirectTransient
    private GridNearAtomicUpdateResponse res;

    /** Maximum possible size of inner collections. */
    @GridDirectTransient
    private int initSize;

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
     * @param topLocked Topology locked flag.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param expiryPlc Expiry policy.
     * @param invokeArgs Optional arguments for entry processor.
     * @param filter Optional filter for atomic check.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     * @param maxEntryCnt Maximum entries count.
     */
    public GridNearAtomicUpdateRequest(
        int cacheId,
        UUID nodeId,
        long futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean topLocked,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Object[] invokeArgs,
        @Nullable CacheEntryPredicate[] filter,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        boolean clientReq,
        boolean addDepInfo,
        int maxEntryCnt
    ) {
//        assert futVer != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futVer = futVer;
        this.fastMap = fastMap;
        this.updateVer = updateVer;

        this.topVer = topVer;
        this.topLocked = topLocked;
        this.syncMode = syncMode;
        this.op = op;
        this.retval = retval;
//        this.expiryPlc = expiryPlc;
//        this.invokeArgs = invokeArgs;
//        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.clientReq = clientReq;
        this.addDepInfo = addDepInfo;

        // By default ArrayList expands to array of 10 elements on first add. We cannot guess how many entries
        // will be added to request because of unknown affinity distribution. However, we DO KNOW how many keys
        // participate in request. As such, we know upper bound of all collections in request. If this bound is lower
        // than 10, we use it.
        initSize = Math.min(maxEntryCnt, 10);

        keys = new ArrayList<>(initSize);
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
    public long futureVersion() {
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
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Topology locked flag.
     */
    public boolean topologyLocked() {
        return topLocked;
    }

    /**
     * @return {@code True} if request sent from client node.
     */
    public boolean clientRequest() {
        return clientReq;
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
        return null;
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
    @Nullable public CacheEntryPredicate[] filter() {
        return null;
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary) {
        EntryProcessor<Object, Object, Object> entryProcessor = null;

        if (op == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProcessor = (EntryProcessor<Object, Object, Object>)val;
        }

        assert val != null || op == DELETE;

        keys.add(key);

        if (entryProcessor != null) {
//            if (entryProcessors == null)
//                entryProcessors = new ArrayList<>(initSize);
//
//            entryProcessors.add(entryProcessor);
        }
        else if (val != null) {
            assert val instanceof CacheObject : val;

            if (vals == null)
                vals = new ArrayList<>(initSize);

            vals.add((CacheObject)val);
        }

        hasPrimary |= primary;
    }

    /**
     * @return Keys for this update request.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @return Values for this update request.
     */
    public List<?> values() {
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
        return null;
    }

    /**
     * @param idx Key index.
     * @return Value.
     */
    @SuppressWarnings("unchecked")
    public CacheObject value(int idx) {
        assert op == UPDATE : op;

        return vals.get(idx);
    }

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @SuppressWarnings("unchecked")
    public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert op == TRANSFORM : op;

        return null;
    }

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public CacheObject writeValue(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /**
     * @return Conflict versions.
     */
    @Nullable public List<GridCacheVersion> conflictVersions() {
        return null;
    }

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public GridCacheVersion conflictVersion(int idx) {
        return null;
    }

    /**
     * @param idx Index.
     * @return Conflict TTL.
     */
    public long conflictTtl(int idx) {
        return CU.TTL_NOT_CHANGED;
    }

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public long conflictExpireTime(int idx) {
        return CU.EXPIRE_TIME_CALCULATE;
    }

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    public boolean hasPrimary() {
        return hasPrimary;
    }

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    public boolean onResponse(GridNearAtomicUpdateResponse res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /**
     * @return Response.
     */
    @Nullable public GridNearAtomicUpdateResponse response() {
        return res;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);

        if (op == TRANSFORM) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

//            if (entryProcessorsBytes == null)
//                entryProcessorsBytes = marshalCollection(entryProcessors, cctx);
//
//            if (invokeArgsBytes == null)
//                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
        }
        else
            prepareMarshalCacheObjects(vals, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        if (op == TRANSFORM) {
//            if (entryProcessors == null)
//                entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);
//
//            if (invokeArgs == null)
//                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }
        else
            finishUnmarshalCacheObjects(vals, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
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

        byte flags = 0;
        if (clientReq)
            flags |= 1;
        if (fastMap)
            flags |= 2;
        if (hasPrimary)
            flags |= 4;
        if (keepBinary)
            flags |= 8;
        if (retval)
            flags |= 16;
        if (skipStore)
            flags |= 32;
        if (topLocked)
            flags |= 64;

        switch (writer.state()) {
            case 3:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 4:
                writer.incrementState();

            case 5:
                writer.incrementState();

            case 6:
                writer.incrementState();

            case 7:
                writer.incrementState();

            case 8:
                writer.incrementState();

            case 9:
                writer.incrementState();

            case 10:
                writer.incrementState();

            case 11:
                if (!writer.writeLong("futVer", futVer))
                    return false;

                writer.incrementState();

            case 12:
                writer.incrementState();

            case 13:
                writer.incrementState();

            case 14:
                writer.incrementState();

            case 15:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 17:
                writer.incrementState();

            case 18:
                writer.incrementState();

            case 19:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 22:
                writer.incrementState();

            case 23:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMessage("updateVer", updateVer))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeCollection("vals", vals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        FUT_VERS.get().add(new T2<Long, UUID>(futVer, nodeId));

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        byte flags = 0;

        switch (reader.state()) {
            case 3:
                flags = reader.readByte("clientReq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                reader.incrementState();

            case 5:
                reader.incrementState();

            case 6:
                reader.incrementState();

            case 7:
                reader.incrementState();

            case 8:
                reader.incrementState();

            case 9:
                reader.incrementState();

            case 10:
                reader.incrementState();

            case 11:
                futVer = reader.readLong("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                reader.incrementState();

            case 13:
                reader.incrementState();

            case 14:
                reader.incrementState();

            case 15:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 17:
                reader.incrementState();

            case 18:
                reader.incrementState();

            case 19:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 21:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                reader.incrementState();

            case 23:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                updateVer = reader.readMessage("updateVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        if ((flags & 1) != 0)
            clientReq = true;
        if ((flags & 2) != 0)
            fastMap = true;
        if ((flags & 4) != 0)
            hasPrimary = true;
        if ((flags & 8) != 0)
            keepBinary = true;
        if ((flags & 16) != 0)
            retval = true;
        if ((flags & 32) != 0)
            skipStore = true;
        if ((flags & 64) != 0)
            topLocked = true;

        if (futVer != 0 && futVer % SAMPLE_MOD == 0) {
            long rcvTimestamp = RECEIVED_TIMESTAMP.get();
            if (rcvTimestamp != 0) {
                String k = G.localIgnite().cluster().localNode().id() + " : " + futVer;
                RECEIVED.putIfAbsent(k, rcvTimestamp);
            }
        }

        return reader.afterMessageRead(GridNearAtomicUpdateRequest.class);
    }

    /**
     * Cleanup values.
     *
     * @param clearKeys If {@code true} clears keys.
     */
    public void cleanup(boolean clearKeys) {
        vals = null;
//        entryProcessors = null;
//        entryProcessorsBytes = null;
//        invokeArgs = null;
//        invokeArgsBytes = null;

        if (clearKeys)
            keys = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateRequest.class, this, "parent", super.toString());
    }
}
