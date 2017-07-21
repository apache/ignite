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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;

/**
 * This class implements stream parser based on {@link GridNioDelimitedBuffer}.
 * <p>
 * The rule for this parser is that every message sent over the stream is appended with
 * delimiter (bytes array). So, the stream structure is as follows:
 * <pre>
 *     +--+--+...+--+--+--+--+--+--+--+...+--+--+--+--+--+-
 *     |   MESSAGE  | DELIMITER  |  MESSAGE  | DELIMITER  |
 *     +--+--+...+--+--+--+--+--+--+--+...+--+--+--+--+--+-
 * </pre>
 */
public class GridDelimitedParser implements GridNioParser {
    /** Buffer metadata key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Delimiter. */
    private final byte[] delim;

    /** Direct buffer. */
    private final boolean directBuf;

    /**
     * @param delim Delimiter.
     * @param directBuf Direct buffer.
     */
    public GridDelimitedParser(byte[] delim, boolean directBuf) {
        this.delim = delim;
        this.directBuf = directBuf;
    }

    /** {@inheritDoc} */
    @Override public byte[] decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
        GridNioDelimitedBuffer nioBuf = ses.meta(BUF_META_KEY);

        // Decode for a given session is called per one thread, so there should not be any concurrency issues.
        // However, we make some additional checks.
        if (nioBuf == null) {
            nioBuf = new GridNioDelimitedBuffer(delim);

            GridNioDelimitedBuffer old = ses.addMeta(BUF_META_KEY, nioBuf);

            assert old == null;
        }

        return nioBuf.read(buf);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        byte[] msg0 = (byte[])msg;

        int cap = msg0.length + delim.length;
        ByteBuffer res = directBuf ? ByteBuffer.allocateDirect(cap) : ByteBuffer.allocate(cap);

        res.put(msg0);
        res.put(delim);

        res.flip();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return this.getClass().getSimpleName();
    }
}