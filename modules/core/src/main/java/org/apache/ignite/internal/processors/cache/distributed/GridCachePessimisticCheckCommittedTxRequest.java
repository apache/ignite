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
    private boolean nearOnlyCheck;

    /** System transaction flag. */
    private boolean sys;

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
        sys = tx.system();

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

    /**
     * @return System transaction flag.
     */
    public boolean system() {
        return sys;
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
        _clone.nearXidVer = nearXidVer != null ? (GridCacheVersion)nearXidVer.clone() : null;
        _clone.originatingNodeId = originatingNodeId;
        _clone.originatingThreadId = originatingThreadId;
        _clone.nearOnlyCheck = nearOnlyCheck;
        _clone.sys = sys;
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
            case 8:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                state++;

            case 9:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                state++;

            case 10:
                if (!writer.writeBoolean("nearOnlyCheck", nearOnlyCheck))
                    return false;

                state++;

            case 11:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                state++;

            case 12:
                if (!writer.writeUuid("originatingNodeId", originatingNodeId))
                    return false;

                state++;

            case 13:
                if (!writer.writeLong("originatingThreadId", originatingThreadId))
                    return false;

                state++;

            case 14:
                if (!writer.writeBoolean("sys", sys))
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
            case 8:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 9:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 10:
                nearOnlyCheck = reader.readBoolean("nearOnlyCheck");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 11:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 12:
                originatingNodeId = reader.readUuid("originatingNodeId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 13:
                originatingThreadId = reader.readLong("originatingThreadId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 14:
                sys = reader.readBoolean("sys");

                if (!reader.isLastRead())
                    return false;

                state++;

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
