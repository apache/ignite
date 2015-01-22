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

import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Request for single partition info.
 */
abstract class GridDhtPartitionsAbstractMessage<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exchange ID. */
    private GridDhtPartitionExchangeId exchId;

    /** Last used cache version. */
    private GridCacheVersion lastVer;

    /**
     * Required by {@link Externalizable}.
     */
    protected GridDhtPartitionsAbstractMessage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @param exchId Exchange ID.
     * @param lastVer Last version.
     */
    GridDhtPartitionsAbstractMessage(GridDhtPartitionExchangeId exchId, @Nullable GridCacheVersion lastVer) {
        this.exchId = exchId;
        this.lastVer = lastVer;
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Last used version among all nodes.
     */
    @Nullable public GridCacheVersion lastVersion() {
        return lastVer;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtPartitionsAbstractMessage _clone = (GridDhtPartitionsAbstractMessage)_msg;

        _clone.exchId = exchId;
        _clone.lastVer = lastVer;
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
                if (!commState.putDhtPartitionExchangeId(exchId))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putCacheVersion(lastVer))
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
                GridDhtPartitionExchangeId exchId0 = commState.getDhtPartitionExchangeId();

                if (exchId0 == DHT_PART_EXCHANGE_ID_NOT_READ)
                    return false;

                exchId = exchId0;

                commState.idx++;

            case 4:
                GridCacheVersion lastVer0 = commState.getCacheVersion();

                if (lastVer0 == CACHE_VER_NOT_READ)
                    return false;

                lastVer = lastVer0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsAbstractMessage.class, this, super.toString());
    }
}
