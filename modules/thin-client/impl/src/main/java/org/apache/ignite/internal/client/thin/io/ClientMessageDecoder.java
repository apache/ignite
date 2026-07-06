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

package org.apache.ignite.internal.client.thin.io;

import java.nio.ByteBuffer;

/**
 * Decodes thin client messages from partial buffers.
 */
public class ClientMessageDecoder {
    /** */
    private byte[] data;

    /** */
    private int cnt = -4;

    /** */
    private int msgSize;

    /**
     * Applies the next partial buffer.
     *
     * @param buf Buffer.
     * @return Decoded message, or null when not yet complete.
     */
    public byte[] apply(ByteBuffer buf) {
        boolean msgReady = read(buf);

        return msgReady ? data : null;
    }

    /**
     * Reads the buffer.
     *
     * @param buf Buffer.
     * @return True when a complete message has been received; false otherwise.
     */
    @SuppressWarnings("DuplicatedCode") // A little duplication is better than a little dependency.
    private boolean read(ByteBuffer buf) {
        if (cnt < 0) {
            for (; cnt < 0 && buf.hasRemaining(); cnt++)
                msgSize |= (buf.get() & 0xFF) << (8 * (4 + cnt));

            if (cnt < 0)
                return false;

            data = new byte[msgSize];
        }

        assert data != null;
        assert cnt >= 0;
        assert msgSize > 0;

        int remaining = buf.remaining();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

                buf.get(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            cnt = -4;
            msgSize = 0;

            return true;
        }

        return false;
    }
}
