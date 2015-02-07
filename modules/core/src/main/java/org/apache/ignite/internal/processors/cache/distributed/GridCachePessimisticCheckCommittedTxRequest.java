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

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Message sent to check that transactions related to some pessimistic transaction
 * were prepared on remote node.
 */
public class GridCachePessimisticCheckCommittedTxRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Originating node ID. */
    private UUID originatingNodeId;

    /** Originating thread ID. */
    private long originatingThreadId;

    /** Flag indicating that this is near-only check. */
    @GridDirectVersion(1)
    private boolean nearOnlyCheck;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCachePessimisticCheckCommittedTxRequest() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param originatingThreadId Originating thread ID.
     * @param futId Future ID.
     */
    public GridCachePessimisticCheckCommittedTxRequest(IgniteInternalTx<K, V> tx, long originatingThreadId, IgniteUuid futId,
        boolean nearOnlyCheck) {
        super(tx.xidVersion(), 0);

        this.futId = futId;
        this.nearOnlyCheck = nearOnlyCheck;

        nearXidVer = tx.nearXidVersion();
        originatingNodeId = tx.eventNodeId();
        this.originatingThreadId = originatingThreadId;
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Tx originating node ID.
     */
    public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /**
     * @return Tx originating thread ID.
     */
    public long originatingThreadId() {
        return originatingThreadId;
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
     * @param miniId Mini ID to set.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Flag indicating that this request was sent only to near node. If this flag is set, no finalizing
     *      will be executed on receiving (near) node since this is a user node.
     */
    public boolean nearOnlyCheck() {
        return nearOnlyCheck;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridCachePessimisticCheckCommittedTxRequest _clone = new GridCachePessimisticCheckCommittedTxRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridCachePessimisticCheckCommittedTxRequest _clone = (GridCachePessimisticCheckCommittedTxRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.nearXidVer = nearXidVer;
        _clone.originatingNodeId = originatingNodeId;
        _clone.originatingThreadId = originatingThreadId;
        _clone.nearOnlyCheck = nearOnlyCheck;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 8:
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid("miniId", miniId))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putCacheVersion("nearXidVer", nearXidVer))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putUuid("originatingNodeId", originatingNodeId))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putLong("originatingThreadId", originatingThreadId))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putBoolean("nearOnlyCheck", nearOnlyCheck))
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
                futId = commState.getGridUuid("futId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 9:
                miniId = commState.getGridUuid("miniId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 10:
                nearXidVer = commState.getCacheVersion("nearXidVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 11:
                originatingNodeId = commState.getUuid("originatingNodeId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 12:
                originatingThreadId = commState.getLong("originatingThreadId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 13:
                nearOnlyCheck = commState.getBoolean("nearOnlyCheck");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 18;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePessimisticCheckCommittedTxRequest.class, this, "super", super.toString());
    }
}
