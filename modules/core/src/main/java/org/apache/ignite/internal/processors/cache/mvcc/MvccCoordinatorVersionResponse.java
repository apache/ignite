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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class MvccCoordinatorVersionResponse implements MvccCoordinatorMessage, MvccCoordinatorVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long futId;

    /** */
    private long crdVer;

    /** */
    private long cntr;

    /** */
    private GridLongList txs; // TODO IGNITE-3478 (do not send on backups?)

    /** */
    private long cleanupVer;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public MvccCoordinatorVersionResponse() {
        // No-op.
    }

    /**
     * @param cntr Counter.
     * @param futId Future ID.
     */
    public MvccCoordinatorVersionResponse(long futId, long crdVer, long cntr, GridLongList txs, long cleanupVer) {
        this.futId = futId;
        this.crdVer = crdVer;
        this.cntr = cntr;
        this.txs = txs;
        this.cleanupVer = cleanupVer;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean processedFromNioThread() {
        return false;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public long cleanupVersion() {
        return cleanupVer;
    }

    /** {@inheritDoc} */
    public long counter() {
        return cntr;
    }

    /** {@inheritDoc} */
    @Override public GridLongList activeTransactions() {
        return txs;
    }

    /** {@inheritDoc} */
    @Override public long coordinatorVersion() {
        return crdVer;
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
                if (!writer.writeLong("cleanupVer", cleanupVer))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("cntr", cntr))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("crdVer", crdVer))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 4:
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
                cleanupVer = reader.readLong("cleanupVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                cntr = reader.readLong("cntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                crdVer = reader.readLong("crdVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                txs = reader.readMessage("txs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccCoordinatorVersionResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 136;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccCoordinatorVersionResponse.class, this);
    }
}
