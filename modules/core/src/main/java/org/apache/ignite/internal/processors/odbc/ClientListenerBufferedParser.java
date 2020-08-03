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

package org.apache.ignite.internal.processors.odbc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;

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

        return nioBuf.read(buf);
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
