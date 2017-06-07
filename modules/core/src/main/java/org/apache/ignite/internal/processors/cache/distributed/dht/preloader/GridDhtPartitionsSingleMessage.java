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

import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.io.Externalizable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of a single node.
 */
public class GridDhtPartitionsSingleMessage extends GridDhtPartitionsAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local partitions. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, GridDhtPartitionMap> parts;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = Integer.class)
    private Map<Integer, Integer> dupPartsData;

    /** Serialized partitions. */
    private byte[] partsBytes;

    /** Partitions update counters. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Map<Integer, T2<Long, Long>>> partCntrs;

    /** Serialized partitions counters. */
    private byte[] partCntrsBytes;

    /** Exception. */
    @GridToStringInclude
    @GridDirectTransient
    private Exception ex;

    /** */
    private byte[] exBytes;

    /** */
    private boolean client;

    /** */
    @GridDirectTransient
    private transient boolean compress;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsSingleMessage() {
        // No-op.
    }

    /**
     * @param exchId Exchange ID.
     * @param client Client message flag.
     * @param lastVer Last version.
     * @param compress {@code True} if it is possible to use compression for message.
     */
    public GridDhtPartitionsSingleMessage(GridDhtPartitionExchangeId exchId,
        boolean client,
        @Nullable GridCacheVersion lastVer,
        boolean compress) {
        super(exchId, lastVer);

        this.client = client;
        this.compress = compress;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /**
     * @return {@code True} if sent from client node.
     */
    public boolean client() {
        return client;
    }

    /**
     * @param cacheId Cache ID to add local partition for.
     * @param locMap Local partition map.
     * @param dupDataCache Optional ID of cache with the same partition state map.
     */
    public void addLocalPartitionMap(int cacheId, GridDhtPartitionMap locMap, @Nullable Integer dupDataCache) {
        if (parts == null)
            parts = new HashMap<>();

        parts.put(cacheId, locMap);

        if (dupDataCache != null) {
            assert compress;
            assert F.isEmpty(locMap.map());
            assert parts.containsKey(dupDataCache);

            if (dupPartsData == null)
                dupPartsData = new HashMap<>();

            dupPartsData.put(cacheId, dupDataCache);
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param cntrMap Partition update counters.
     */
    public void partitionUpdateCounters(int grpId, Map<Integer, T2<Long, Long>> cntrMap) {
        if (partCntrs == null)
            partCntrs = new HashMap<>();

        partCntrs.put(grpId, cntrMap);
    }

    /**
     * @param grpId Cache group ID.
     * @return Partition update counters.
     */
    @Override public Map<Integer, T2<Long, Long>> partitionUpdateCounters(int grpId) {
        if (partCntrs != null) {
            Map<Integer, T2<Long, Long>> res = partCntrs.get(grpId);

            return res != null ? res : Collections.<Integer, T2<Long, Long>>emptyMap();
        }

        return Collections.emptyMap();
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionMap> partitions() {
        if (parts == null)
            parts = new HashMap<>();

        return parts;
    }

    /**
     * @param ex Exception.
     */
    public void setException(Exception ex) {
        this.ex = ex;
    }

    /**
     *
     */
    public Exception getException() {
        return ex;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        boolean marshal = (parts != null && partsBytes == null) ||
            (partCntrs != null && partCntrsBytes == null) ||
            (ex != null && exBytes == null);

        if (marshal) {
            byte[] partsBytes0 = null;
            byte[] partCntrsBytes0 = null;
            byte[] exBytes0 = null;

            if (parts != null && partsBytes == null)
                partsBytes0 = U.marshal(ctx, parts);

            if (partCntrs != null && partCntrsBytes == null)
                partCntrsBytes0 = U.marshal(ctx, partCntrs);

            if (ex != null && exBytes == null)
                exBytes0 = U.marshal(ctx, ex);

            if (compress) {
                assert !compressed();

                try {
                    byte[] partsBytesZip = U.zip(partsBytes0);
                    byte[] partCntrsBytesZip = U.zip(partCntrsBytes0);
                    byte[] exBytesZip = U.zip(exBytes0);

                    partsBytes0 = partsBytesZip;
                    partCntrsBytes0 = partCntrsBytesZip;
                    exBytes0 = exBytesZip;

                    compressed(true);
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.logger(getClass()), "Failed to compress partitions data: " + e, e);
                }
            }

            partsBytes = partsBytes0;
            partCntrsBytes = partCntrsBytes0;
            exBytes = exBytes0;
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null && parts == null) {
            if (compressed())
                parts = U.unmarshalZip(ctx.marshaller(), partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                parts = U.unmarshal(ctx, partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (partCntrsBytes != null && partCntrs == null) {
            if (compressed())
                partCntrs = U.unmarshalZip(ctx.marshaller(), partCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                partCntrs = U.unmarshal(ctx, partCntrsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (exBytes != null && ex == null) {
            if (compressed())
                ex = U.unmarshalZip(ctx.marshaller(), exBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
            else
                ex = U.unmarshal(ctx, exBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
        }

        if (dupPartsData != null) {
            assert parts != null;

            for (Map.Entry<Integer, Integer> e : dupPartsData.entrySet()) {
                GridDhtPartitionMap map1 = parts.get(e.getKey());

                assert map1 != null : e.getKey();
                assert F.isEmpty(map1.map());
                assert !map1.hasMovingPartitions();

                GridDhtPartitionMap map2 = parts.get(e.getValue());

                assert map2 != null : e.getValue();
                assert map2.map() != null;

                for (Map.Entry<Integer, GridDhtPartitionState> e0 : map2.map().entrySet())
                    map1.put(e0.getKey(), e0.getValue());
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
            case 5:
                if (!writer.writeBoolean("client", client))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap("dupPartsData", dupPartsData, MessageCollectionItemType.INT, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByteArray("exBytes", exBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByteArray("partCntrsBytes", partCntrsBytes))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByteArray("partsBytes", partsBytes))
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
            case 5:
                client = reader.readBoolean("client");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                dupPartsData = reader.readMap("dupPartsData", MessageCollectionItemType.INT, MessageCollectionItemType.INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                exBytes = reader.readByteArray("exBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                partCntrsBytes = reader.readByteArray("partCntrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                partsBytes = reader.readByteArray("partsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionsSingleMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 47;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsSingleMessage.class, this, super.toString());
    }
}
