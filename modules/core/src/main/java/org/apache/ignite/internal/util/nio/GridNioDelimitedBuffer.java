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
import java.util.Arrays;
import org.jetbrains.annotations.Nullable;

/**
 * Buffer with message delimiter support.
 */
public class GridNioDelimitedBuffer {
    /** Delimiter. */
    private final byte[] delim;

    /** Data. */
    private byte[] data = new byte[16384];

    /** Count. */
    private int cnt;

    /** Index. */
    private int idx;

    /**
     * @param delim Delimiter.
     */
    public GridNioDelimitedBuffer(byte[] delim) {
        assert delim != null;
        assert delim.length > 0;

        this.delim = delim;

        reset();
    }

    /**
     * Resets buffer state.
     */
    private void reset() {
        cnt = 0;
        idx = 0;
    }

    /**
     * @param buf Buffer.
     * @return Message bytes or {@code null} if message is not fully read yet.
     */
    @Nullable public byte[] read(ByteBuffer buf) {
        while(buf.hasRemaining()) {
            if (cnt == data.length)
                data = Arrays.copyOf(data, data.length * 2);

            byte b = buf.get();

            data[cnt++] = b;

            if (b == delim[idx])
                idx++;
            else if (idx > 0) {
                int pos = cnt - idx;

                idx = 0;

                for (int i = pos; i < cnt; i++) {
                    if (data[pos] == delim[idx]) {
                        pos++;

                        idx++;
                    }
                    else {
                        pos = cnt - (i - pos) - 1;

                        idx = 0;
                    }
                }
            }

            if (idx == delim.length) {
                byte[] bytes = Arrays.copyOfRange(data, 0, cnt - delim.length);

                reset();

                return bytes;
            }
        }

        return null;
    }
}