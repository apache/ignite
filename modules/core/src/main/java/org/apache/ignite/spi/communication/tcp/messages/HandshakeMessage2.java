/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Updated handshake message.
 */
@IgniteCodeGeneratingFail
public class HandshakeMessage2 extends HandshakeMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int connIdx;

    /**
     *
     */
    public HandshakeMessage2() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param connectCnt Connect count.
     * @param rcvCnt Number of received messages.
     * @param connIdx Connection index.
     */
    public HandshakeMessage2(UUID nodeId, long connectCnt, long rcvCnt, int connIdx) {
        super(nodeId, connectCnt, rcvCnt);

        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -44;
    }

    /** {@inheritDoc} */
    @Override public int connectionIndex() {
        return connIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (!super.writeTo(buf, writer))
            return false;

        if (buf.remaining() < 4)
            return false;

        buf.putInt(connIdx);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (!super.readFrom(buf, reader))
            return false;

        if (buf.remaining() < 4)
            return false;

        connIdx = buf.getInt();

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeMessage2.class, this);
    }
}
