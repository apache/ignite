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
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class MvccTxInfo implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID crd;

    /** */
    private MvccVersion mvccVer;

    /**
     *
     */
    public MvccTxInfo() {
        // No-op.
    }

    /**
     * @param crd Coordinator node ID.
     * @param mvccVer Mvcc version.
     */
    public MvccTxInfo(UUID crd, MvccVersion mvccVer) {
        assert crd != null;
        assert mvccVer != null;

        this.crd = crd;
        this.mvccVer = mvccVer;
    }

    /**
     * @return Instance with version without active transactions.
     */
    public MvccTxInfo withoutActiveTransactions() {
        MvccVersion mvccVer0 = mvccVer.withoutActiveTransactions();

        if (mvccVer0 == mvccVer)
            return this;

        return new MvccTxInfo(crd, mvccVer0);
    }

    /**
     * @return Coordinator node ID.
     */
    public UUID coordinatorNodeId() {
        return crd;
    }

    /**
     * @return Mvcc version.
     */
    public MvccVersion version() {
        return mvccVer;
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
                if (!writer.writeUuid("crd", crd))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("mvccVer", mvccVer))
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
                crd = reader.readUuid("crd");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                mvccVer = reader.readMessage("mvccVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccTxInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 139;
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
        return S.toString(MvccTxInfo.class, this);
    }
}
