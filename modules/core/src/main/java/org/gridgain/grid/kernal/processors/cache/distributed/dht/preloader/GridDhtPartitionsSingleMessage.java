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

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Information about partitions of a single node.
 */
public class GridDhtPartitionsSingleMessage<K, V> extends GridDhtPartitionsAbstractMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local partitions. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, GridDhtPartitionMap> parts = new HashMap<>();

    /** Serialized partitions. */
    private byte[] partsBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsSingleMessage() {
        // No-op.
    }

    /**
     * @param exchId Exchange ID.
     * @param lastVer Last version.
     */
    public GridDhtPartitionsSingleMessage(GridDhtPartitionExchangeId exchId, @Nullable GridCacheVersion lastVer) {
        super(exchId, lastVer);
    }

    /**
     * Adds partition map to this message.
     *
     * @param cacheId Cache ID to add local partition for.
     * @param locMap Local partition map.
     */
    public void addLocalPartitionMap(int cacheId, GridDhtPartitionMap locMap) {
        parts.put(cacheId, locMap);
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionMap> partitions() {
        return parts;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (parts != null)
            partsBytes = ctx.marshaller().marshal(parts);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null)
            parts = ctx.marshaller().unmarshal(partsBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtPartitionsSingleMessage _clone = new GridDhtPartitionsSingleMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtPartitionsSingleMessage _clone = (GridDhtPartitionsSingleMessage)_msg;

        _clone.parts = parts;
        _clone.partsBytes = partsBytes;
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
            case 5:
                if (!commState.putByteArray(partsBytes))
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
            case 5:
                byte[] partsBytes0 = commState.getByteArray();

                if (partsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                partsBytes = partsBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 46;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsSingleMessage.class, this, super.toString());
    }
}
