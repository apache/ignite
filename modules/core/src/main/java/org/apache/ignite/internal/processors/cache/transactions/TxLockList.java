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

package org.apache.ignite.internal.processors.cache.transactions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * List of transaction locks for particular key.
 */
public class TxLockList implements Message {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Tx locks. */
    @GridToStringInclude
    @GridDirectCollection(value = TxLock.class)
    private List<TxLock> txLocks = new ArrayList<>();

    /**
     * Default constructor.
     */
    public TxLockList() {
        // No-op.
    }

    /**
     * @return Lock list.
     */
    public List<TxLock> txLocks() {
        return txLocks;
    }

    /**
     * @param txLock Tx lock.
     */
    public void add(TxLock txLock) {
        txLocks.add(txLock);
    }

    /**
     * @return {@code True} if lock list is empty.
     */
    public boolean isEmpty() {
        return txLocks.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLockList.class, this);
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
                if (!writer.writeCollection("txLocks", txLocks, MessageCollectionItemType.MSG))
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
                txLocks = reader.readCollection("txLocks", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(TxLockList.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -26;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
