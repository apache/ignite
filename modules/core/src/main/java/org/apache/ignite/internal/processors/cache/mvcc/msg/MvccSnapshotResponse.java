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
import java.util.Arrays;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.mvcc.MvccLongList;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class MvccSnapshotResponse implements MvccMessage, MvccSnapshot, MvccLongList {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long futId;

    /** */
    private long crdVer;

    /** */
    private long cntr;

    /** */
    private int opCntr;

    /** */
    @GridDirectTransient
    private int txsCnt;

    /** */
    private long[] txs;

    /** */
    private long cleanupVer;

    /** */
    @GridDirectTransient
    private long tracking;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public MvccSnapshotResponse() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param crdVer Coordinator version.
     * @param cntr Counter.
     * @param opCntr Operation counter.
     * @param cleanupVer Cleanup version.
     * @param tracking Tracking number.
     */
    public void init(long futId, long crdVer, long cntr, int opCntr, long cleanupVer, long tracking) {
        this.futId = futId;
        this.crdVer = crdVer;
        this.cntr = cntr;
        this.opCntr = opCntr;
        this.cleanupVer = cleanupVer;
        this.tracking = tracking;

        if (txsCnt > 0 && txs.length > txsCnt) // truncate if necessary
            txs = Arrays.copyOf(txs, txsCnt);
    }

    /**
     * @param txId Transaction counter.
     */
    public void addTx(long txId) {
        if (txs == null)
            txs = new long[4];
        else if (txs.length == txsCnt)
            txs = Arrays.copyOf(txs, txs.length << 1);

        txs[txsCnt++] = txId;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return txsCnt;
    }

    /** {@inheritDoc} */
    @Override public long get(int i) {
        return txs[i];
    }

    /** {@inheritDoc} */
    @Override public boolean contains(long val) {
        for (int i = 0; i < txsCnt; i++) {
            if (txs[i] == val)
                return true;
        }

        return false;
    }

    /**
     * @return Tracking counter.
     */
    public long tracking() {
        return tracking;
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
    @Override public long counter() {
        return cntr;
    }

    /** {@inheritDoc} */
    @Override public int operationCounter() {
        return opCntr;
    }

    /** {@inheritDoc} */
    @Override public void incrementOperationCounter() {
        opCntr++;
    }

    /** {@inheritDoc} */
    @Override public MvccLongList activeTransactions() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot withoutActiveTransactions() {
        if (txsCnt > 0)
            return new MvccSnapshotWithoutTxs(crdVer, cntr, opCntr, cleanupVer);

        return this;
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
                if (!writer.writeInt("opCntr", opCntr))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLongArray("txs", txs))
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
                opCntr = reader.readInt("opCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                txs = reader.readLongArray("txs");

                if (!reader.isLastRead())
                    return false;

                txsCnt = txs != null ? txs.length : 0;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccSnapshotResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 141;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccSnapshotResponse.class, this);
    }
}
