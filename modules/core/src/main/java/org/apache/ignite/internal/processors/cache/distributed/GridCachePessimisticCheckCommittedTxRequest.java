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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeField("futId", futId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("miniId", miniId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeField("nearOnlyCheck", nearOnlyCheck, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeField("nearXidVer", nearXidVer, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeField("originatingNodeId", originatingNodeId, MessageFieldType.UUID))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeField("originatingThreadId", originatingThreadId, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeField("sys", sys, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 8:
                futId = reader.readField("futId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                miniId = reader.readField("miniId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                nearOnlyCheck = reader.readField("nearOnlyCheck", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                nearXidVer = reader.readField("nearXidVer", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                originatingNodeId = reader.readField("originatingNodeId", MessageFieldType.UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                originatingThreadId = reader.readField("originatingThreadId", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                sys = reader.readField("sys", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

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
