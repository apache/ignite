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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class CoordinatorAckRequestTxAndQueryEx extends CoordinatorAckRequestTx {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long qryCrdVer;

    /** */
    private long qryCntr;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public CoordinatorAckRequestTxAndQueryEx() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param txCntr Counter assigned to transaction update.
     * @param qryCrdVer Version of coordinator assigned read counter.
     * @param qryCntr Counter assigned for transaction reads.
     */
    CoordinatorAckRequestTxAndQueryEx(long futId, long txCntr, long qryCrdVer, long qryCntr) {
        super(futId, txCntr);

        this.qryCrdVer = qryCrdVer;
        this.qryCntr = qryCntr;
    }

    /** {@inheritDoc} */
    @Override long queryCoordinatorVersion() {
        return qryCrdVer;
    }

    /** {@inheritDoc} */
    @Override long queryCounter() {
        return qryCntr;
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
            case 3:
                if (!writer.writeLong("qryCntr", qryCntr))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("qryCrdVer", qryCrdVer))
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
            case 3:
                qryCntr = reader.readLong("qryCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                qryCrdVer = reader.readLong("qryCrdVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CoordinatorAckRequestTxAndQueryEx.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 142;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CoordinatorAckRequestTxAndQueryEx.class, this);
    }
}
