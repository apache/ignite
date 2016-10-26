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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicFullUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** Keys to update. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** Values to update. */
    @GridDirectCollection(CacheObject.class)
    private List<CacheObject> vals;

    /** Partitions of keys. */
    @GridDirectCollection(int.class)
    private List<Integer> partIds;

    /** Entry processors. */
    @GridDirectTransient
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> entryProcessorsBytes;

    /** Conflict versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> conflictVers;

    /** Conflict TTLs. */
    private GridLongList conflictTtls;

    /** Conflict expire times. */
    private GridLongList conflictExpireTimes;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    protected Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    protected byte[][] invokeArgsBytes;

    /** Maximum possible size of inner collections. */
    @GridDirectTransient
    private int initSize;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicFullUpdateRequest() {
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
    public GridNearAtomicFullUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
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
        super(
            cacheId,
            nodeId,
            futVer,
            fastMap,
            updateVer,
            topVer,
            topLocked,
            syncMode,
            op,
            retval,
            expiryPlc,
            filter,
            subjId,
            taskNameHash,
            skipStore,
            keepBinary,
            clientReq,
            addDepInfo
        );

        // By default ArrayList expands to array of 10 elements on first add. We cannot guess how many entries
        // will be added to request because of unknown affinity distribution. However, we DO KNOW how many keys
        // participate in request. As such, we know upper bound of all collections in request. If this bound is lower
        // than 10, we use it.

        this.invokeArgs = invokeArgs;

        initSize = Math.min(maxEntryCnt, 10);

        keys = new ArrayList<>(initSize);

        partIds = new ArrayList<>(initSize);
    }

    /** {@inheritDoc} */
    @Override public void addUpdateEntry(KeyCacheObject key,
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
        partIds.add(key.partition());

        if (entryProcessor != null) {
            if (entryProcessors == null)
                entryProcessors = new ArrayList<>(initSize);

            entryProcessors.add(entryProcessor);
        }
        else if (val != null) {
            assert val instanceof CacheObject : val;

            if (vals == null)
                vals = new ArrayList<>(initSize);

            vals.add((CacheObject)val);
        }

        hasPrimary |= primary;

        // In case there is no conflict, do not create the list.
        if (conflictVer != null) {
            if (conflictVers == null) {
                conflictVers = new ArrayList<>(initSize);

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictVers.add(null);
            }

            conflictVers.add(conflictVer);
        }
        else if (conflictVers != null)
            conflictVers.add(null);

        if (conflictTtl >= 0) {
            if (conflictTtls == null) {
                conflictTtls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictTtls.add(CU.TTL_NOT_CHANGED);
            }

            conflictTtls.add(conflictTtl);
        }

        if (conflictExpireTime >= 0) {
            if (conflictExpireTimes == null) {
                conflictExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }

            conflictExpireTimes.add(conflictExpireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public List<KeyCacheObject> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return op == TRANSFORM ? entryProcessors : vals;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public CacheObject value(int idx) {
        assert op == UPDATE : op;

        return vals.get(idx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert op == TRANSFORM : op;

        return entryProcessors.get(idx);
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public List<GridCacheVersion> conflictVersions() {
        return conflictVers;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        if (conflictVers != null) {
            assert idx >= 0 && idx < conflictVers.size();

            return conflictVers.get(idx);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        if (conflictTtls != null) {
            assert idx >= 0 && idx < conflictTtls.size();

            return conflictTtls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        if (conflictExpireTimes != null) {
            assert idx >= 0 && idx < conflictExpireTimes.size();

            return conflictExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
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

            if (entryProcessorsBytes == null)
                entryProcessorsBytes = marshalCollection(entryProcessors, cctx);

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
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
            if (entryProcessors == null)
                entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }
        else
            finishUnmarshalCacheObjects(vals, cctx, ldr);

        if (partIds != null && !partIds.isEmpty()) {
            assert partIds.size() == keys.size();

            for (int i = 0; i < keys.size(); i++)
                keys.get(i).partition(partIds.get(i));
        }
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

        switch (writer.state()) {
            case 19:
                if (!writer.writeMessage("conflictExpireTimes", conflictExpireTimes))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMessage("conflictTtls", conflictTtls))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection("conflictVers", conflictVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeCollection("entryProcessorsBytes", entryProcessorsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeCollection("partIds", partIds, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeCollection("vals", vals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 19:
                conflictExpireTimes = reader.readMessage("conflictExpireTimes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                conflictTtls = reader.readMessage("conflictTtls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                conflictVers = reader.readCollection("conflictVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                entryProcessorsBytes = reader.readCollection("entryProcessorsBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                partIds = reader.readCollection("partIds", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicFullUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKeys) {
        vals = null;
        entryProcessors = null;
        entryProcessorsBytes = null;
        invokeArgs = null;
        invokeArgsBytes = null;

        if (clearKeys)
            keys = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 27;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicFullUpdateRequest.class, this, "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
