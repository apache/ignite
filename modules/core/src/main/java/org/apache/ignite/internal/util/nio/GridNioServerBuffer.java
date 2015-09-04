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

package org.apache.ignite.internal.util.nio;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * NIO server buffer.
 */
public class GridNioServerBuffer {
    /** */
    private byte[] data;

    /** */
    private int cnt = -4;

    /** */
    private int msgSize;

    /**
     *
     */
    public void reset() {
        msgSize = 0;
        cnt = -4;
        data = null;
    }

    /**
     * Checks whether the byte array is filled.
     *
     * @return Flag indicating whether byte array is filled or not.
     */
    public boolean isFilled() {
        return cnt > 0 && cnt == msgSize;
    }

    /**
     * Get data withing the buffer.
     *
     * @return Data.
     */
    public byte[] data() {
        return data;
    }

    /**
     * @param buf Buffer.
     * @return Message bytes or {@code null} if message is not fully read yet.
     * @throws IgniteCheckedException If failed to parse message.
     */
    @Nullable public byte[] read(ByteBuffer buf) throws IgniteCheckedException {
        if (cnt < 0) {
            for (; cnt < 0 && buf.hasRemaining(); cnt++) {
                msgSize <<= 8;

                msgSize |= buf.get() & 0xFF;
            }

            if (cnt < 0)
                return null;

            // If count is 0 then message size should be inited.
            if (msgSize <= 0)
                throw new IgniteCheckedException("Invalid message size: " + msgSize);

            data = new byte[msgSize];
        }

        assert msgSize > 0;
        assert cnt >= 0;

        int remaining = buf.remaining();

        // If there are more bytes in buffer.
        if (remaining > 0) {
            int missing = msgSize - cnt;

            // Read only up to message size.
            if (missing > 0) {
                int len = missing < remaining ? missing : remaining;

                buf.get(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            byte[] data0 = data;

            reset();

            return data0;
        }
        else
            return null;
    }
}