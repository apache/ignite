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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class implements stream parser based on {@link ClientListenerNioServerBuffer}.
 * <p>
 * The rule for this parser is that every message sent over the stream is prepended with
 * 4-byte integer header containing message size. So, the stream structure is as follows:
 * <pre>
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 *     | MSG_SIZE  |   MESSAGE  | MSG_SIZE  |   MESSAGE  |
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 * </pre>
 */
public class ClientListenerBufferedParser implements GridNioParser {
    /** Buffer metadata key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** {@inheritDoc} */
    @Override public byte[] decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
        ClientListenerNioServerBuffer nioBuf = ses.meta(BUF_META_KEY);

        // Decode for a given session is called per one thread, so there should not be any concurrency issues.
        // However, we make some additional checks.
        if (nioBuf == null) {
            nioBuf = new ClientListenerNioServerBuffer();

            ClientListenerNioServerBuffer old = ses.addMeta(BUF_META_KEY, nioBuf);

            assert old == null;
        }

        boolean checkHandshake = ses.meta(ClientListenerNioListener.CONN_CTX_HANDSHAKE_PASSED) == null;

        return nioBuf.read(buf, checkHandshake);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        byte[] msg0 = (byte[])msg;

        ByteBuffer res = ByteBuffer.allocate(msg0.length + 4);

        res.order(ByteOrder.LITTLE_ENDIAN);

        res.putInt(msg0.length);
        res.put(msg0);

        res.flip();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return ClientListenerBufferedParser.class.getSimpleName();
    }
}
