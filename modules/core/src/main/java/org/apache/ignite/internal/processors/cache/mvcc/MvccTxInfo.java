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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * TODO: Remove
 */
public class MvccTxInfo implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private MvccSnapshot mvccSnapshot;

    /**
     *
     */
    public MvccTxInfo() {
        // No-op.
    }

    /**
     * @param mvccSnapshot MVCC snapshot.
     */
    public MvccTxInfo(MvccSnapshot mvccSnapshot) {
        assert mvccSnapshot != null;

        this.mvccSnapshot = mvccSnapshot;
    }

    /**
     * @return Instance with version without active transactions.
     */
    public MvccTxInfo withoutActiveTransactions() {
        MvccSnapshot mvccSnapshot0 = mvccSnapshot.withoutActiveTransactions();

        if (mvccSnapshot0 == mvccSnapshot)
            return this;

        return new MvccTxInfo(mvccSnapshot0);
    }

    /**
     * @return Mvcc version.
     */
    public MvccSnapshot snapshot() {
        return mvccSnapshot;
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
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
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
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccTxInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 144;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
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
