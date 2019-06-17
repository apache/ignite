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
import java.util.Collection;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Probe message travelling between transactions (from waiting to blocking) during deadlock detection.
 * @see DeadlockDetectionManager
 */
public class DeadlockProbe implements Message {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private GridCacheVersion initiatorVer;

    /** */
    @GridToStringInclude
    @GridDirectCollection(ProbedTx.class)
    private Collection<ProbedTx> waitChain;

    /** */
    private ProbedTx blocker;

    /** */
    private boolean nearCheck;

    /** */
    public DeadlockProbe() {
    }

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public DeadlockProbe(GridCacheVersion initiatorVer, Collection<ProbedTx> waitChain,
        ProbedTx blocker, boolean nearCheck) {
        this.initiatorVer = initiatorVer;
        this.waitChain = waitChain;
        this.blocker = blocker;
        this.nearCheck = nearCheck;
    }

    /**
     * @return Identifier of a transaction started a deadlock detection process. Can be used for diagnostics.
     */
    public GridCacheVersion initiatorVersion() {
        return initiatorVer;
    }

    /**
     * @return Chain of transactions identified as waiting during deadlock detection.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Collection<ProbedTx> waitChain() {
        return waitChain;
    }

    /**
     * @return Identifier of a transaction identified as blocking last transaction in the wait chain
     * during deadlock deteciton.
     */
    public ProbedTx blocker() {
        return blocker;
    }

    /**
     * @return {@code True} if checks if near transaction is waiting. {@code False} if checks dht transaction.
     */
    public boolean nearCheck() {
        return nearCheck;
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
                if (!writer.writeMessage("blocker", blocker))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("initiatorVer", initiatorVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("nearCheck", nearCheck))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection("waitChain", waitChain, MessageCollectionItemType.MSG))
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
                blocker = reader.readMessage("blocker");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                initiatorVer = reader.readMessage("initiatorVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                nearCheck = reader.readBoolean("nearCheck");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                waitChain = reader.readCollection("waitChain", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(DeadlockProbe.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 170;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DeadlockProbe.class, this);
    }
}
