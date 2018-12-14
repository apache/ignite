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

package org.apache.ignite.internal.processors.cache.mvcc.msg;

import java.nio.ByteBuffer;

import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class MvccWaitTxsRequest implements MvccMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long futId;

    /** */
    private GridLongList txs;

    /**
     *
     */
    public MvccWaitTxsRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param txs Transactions to wait for.
     */
    public MvccWaitTxsRequest(long futId, GridLongList txs) {
        assert txs != null && !txs.isEmpty() : txs;

        this.futId = futId;
        this.txs = txs;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @return Transactions to wait for.
     */
    public GridLongList transactions() {
        return txs;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean processedFromNioThread() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("txs", txs))
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

        switch (reader.state()) {
            case 0:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                txs = reader.readMessage("txs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccWaitTxsRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 142;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccWaitTxsRequest.class, this);
    }
}
