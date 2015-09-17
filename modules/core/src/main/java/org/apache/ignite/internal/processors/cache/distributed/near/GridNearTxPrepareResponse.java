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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Near cache prepare response.
 */
public class GridNearTxPrepareResponse extends GridDistributedTxPrepareResponse {
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

    /** Write version. */
    private GridCacheVersion writeVer;

    /** */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts;

    /** Map of owned values to set on near node. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey, CacheVersionedValue> ownedVals;

    /** OwnedVals' keys for marshalling. */
    @GridToStringExclude
    @GridDirectCollection(IgniteTxKey.class)
    private Collection<IgniteTxKey> ownedValKeys;

    /** OwnedVals' values for marshalling. */
    @GridToStringExclude
    @GridDirectCollection(CacheVersionedValue.class)
    private Collection<CacheVersionedValue> ownedValVals;

    /** Cache return value. */
    private GridCacheReturn retVal;

    /** Filter failed keys. */
    @GridDirectCollection(IgniteTxKey.class)
    private Collection<IgniteTxKey> filterFailedKeys;

    /** Not {@code null} if client node should remap transaction. */
    private AffinityTopologyVersion clientRemapVer;

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
     * @param writeVer Write version.
     * @param retVal Return value.
     * @param err Error.
     * @param clientRemapVer Not {@code null} if client node should remap transaction.
     */
    public GridNearTxPrepareResponse(
        GridCacheVersion xid,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion dhtVer,
        GridCacheVersion writeVer,
        GridCacheReturn retVal,
        Throwable err,
        AffinityTopologyVersion clientRemapVer
    ) {
        super(xid, err);

        assert futId != null;
        assert miniId != null;
        assert dhtVer != null;

        this.futId = futId;
        this.miniId = miniId;
        this.dhtVer = dhtVer;
        this.writeVer = writeVer;
        this.retVal = retVal;
        this.clientRemapVer = clientRemapVer;
    }

    /**
     * @return {@code True} if client node should remap transaction.
     */
    @Nullable public AffinityTopologyVersion clientRemapVersion() {
        return clientRemapVer;
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
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * Adds owned value.
     *
     * @param key Key.
     * @param ver DHT version.
     * @param val Value.
     */
    public void addOwnedValue(IgniteTxKey key, GridCacheVersion ver, CacheObject val) {
        if (val == null)
            return;

        if (ownedVals == null)
            ownedVals = new HashMap<>();

        CacheVersionedValue oVal = new CacheVersionedValue(val, ver);

        ownedVals.put(key, oVal);
    }

    /**
     * @return Owned values map.
     */
    public Map<IgniteTxKey, CacheVersionedValue> ownedValues() {
        return ownedVals == null ?
            Collections.<IgniteTxKey, CacheVersionedValue>emptyMap() :
            Collections.unmodifiableMap(ownedVals);
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn returnValue() {
        return retVal;
    }

    /**
     * @param filterFailedKeys Collection of keys that did not pass the filter.
     */
    public void filterFailedKeys(Collection<IgniteTxKey> filterFailedKeys) {
        this.filterFailedKeys = filterFailedKeys;
    }

    /**
     * @return Collection of keys that did not pass the filter.
     */
    public Collection<IgniteTxKey> filterFailedKeys() {
        return filterFailedKeys == null ? Collections.<IgniteTxKey>emptyList() : filterFailedKeys;
    }

    /**
     * @param key Key.
     * @return {@code True} if response has owned value for given key.
     */
    public boolean hasOwnedValue(IgniteTxKey key) {
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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (ownedVals != null) {
            ownedValKeys = ownedVals.keySet();

            ownedValVals = ownedVals.values();

            for (Map.Entry<IgniteTxKey, CacheVersionedValue> entry : ownedVals.entrySet()) {
                GridCacheContext cacheCtx = ctx.cacheContext(entry.getKey().cacheId());

                entry.getKey().prepareMarshal(cacheCtx);

                entry.getValue().prepareMarshal(cacheCtx.cacheObjectContext());
            }
        }

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.prepareMarshal(cctx);
        }

        if (filterFailedKeys != null) {
            for (IgniteTxKey key :filterFailedKeys) {
                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                key.prepareMarshal(cctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedValKeys != null && ownedVals == null) {
            ownedVals = U.newHashMap(ownedValKeys.size());

            assert ownedValKeys.size() == ownedValVals.size();

            Iterator<IgniteTxKey> keyIter = ownedValKeys.iterator();

            Iterator<CacheVersionedValue> valIter = ownedValVals.iterator();

            while (keyIter.hasNext()) {
                IgniteTxKey key = keyIter.next();

                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                CacheVersionedValue val = valIter.next();

                key.finishUnmarshal(cctx, ldr);

                val.finishUnmarshal(cctx, ldr);

                ownedVals.put(key, val);
            }
        }

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.finishUnmarshal(cctx, ldr);
        }

        if (filterFailedKeys != null) {
            for (IgniteTxKey key :filterFailedKeys) {
                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                key.finishUnmarshal(cctx, ldr);
            }
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
            case 8:
                if (!writer.writeMessage("clientRemapVer", clientRemapVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("dhtVer", dhtVer))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection("filterFailedKeys", filterFailedKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection("invalidParts", invalidParts, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("ownedValKeys", ownedValKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection("ownedValVals", ownedValVals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection("pending", pending, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMessage("retVal", retVal))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMessage("writeVer", writeVer))
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
            case 8:
                clientRemapVer = reader.readMessage("clientRemapVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                dhtVer = reader.readMessage("dhtVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                filterFailedKeys = reader.readCollection("filterFailedKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                invalidParts = reader.readCollection("invalidParts", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                ownedValKeys = reader.readCollection("ownedValKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                ownedValVals = reader.readCollection("ownedValVals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                pending = reader.readCollection("pending", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                retVal = reader.readMessage("retVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxPrepareResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 56;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 19;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareResponse.class, this, "super", super.toString());
    }

}