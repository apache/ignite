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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Check prepared transactions response.
 */
public class GridCacheOptimisticCheckPreparedTxResponse<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Flag indicating if all remote transactions were prepared. */
    private boolean success;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheOptimisticCheckPreparedTxResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param success {@code True} if all remote transactions were prepared, {@code false} otherwise.
     */
    public GridCacheOptimisticCheckPreparedTxResponse(GridCacheVersion txId, IgniteUuid futId, IgniteUuid miniId,
        boolean success) {
        super(txId, 0);

        this.futId = futId;
        this.miniId = miniId;
        this.success = success;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return {@code True} if all remote transactions were prepared.
     */
    public boolean success() {
        return success;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheOptimisticCheckPreparedTxResponse _clone = new GridCacheOptimisticCheckPreparedTxResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheOptimisticCheckPreparedTxResponse _clone = (GridCacheOptimisticCheckPreparedTxResponse)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.success = success;
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
            case 8:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putBoolean(success))
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
            case 8:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 9:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 10:
                if (buf.remaining() < 1)
                    return false;

                success = commState.getBoolean();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 19;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOptimisticCheckPreparedTxResponse.class, this, "super", super.toString());
    }
}
