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

package org.apache.ignite.internal.processors.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

/**
 * Parser to decode/encode Redis protocol requests.
 */
public class GridRedisProtocolParser implements GridNioParser {
    private final IgniteLogger log;

    /** + prefix. */
    private static final byte SIMPLE_STRING = 43;

    /** $ */
    private static final byte BULK_STRING = 36;

    /** : */
    private static final byte INTEGER = 58;

    /** * */
    private static final byte ARRAY = 42;

    /** - */
    private static final byte ERROR = 45;

    /** Carriage return code. */
    private static final byte CR = 13;

    /** Line feed code. */
    private static final byte LF = 10;

    /** CRLF. */
    private static final byte[] CRLF = new byte[] {13, 10};

    public GridRedisProtocolParser(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridRedisMessage decode(GridNioSession ses, ByteBuffer buf)
        throws IOException, IgniteCheckedException {
        return readArray(buf);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg0) throws IOException, IgniteCheckedException {
        assert msg0 != null;

        GridRedisMessage msg = (GridRedisMessage)msg0;

        return msg.getResponse();
    }

    private GridRedisMessage readArray(ByteBuffer buf) throws IgniteCheckedException {
        System.out.println(new String(buf.array()));
        byte b = buf.get();

        if (b != ARRAY)
            throw new IgniteCheckedException("Invalid request byte! " + b);

        int arrLen = elCnt(buf);

        GridRedisMessage msg = new GridRedisMessage(arrLen);

        for (int i = 0; i < arrLen; i++)
            msg.append(readBulkStr(buf));

        return msg;
    }

    private String readBulkStr(ByteBuffer buf) throws IgniteCheckedException {
        byte b = buf.get();

        if (b != BULK_STRING)
            throw new IgniteCheckedException("Invalid bulk string prefix! " + b);

        int len = elCnt(buf);
        byte[] bulkStr = new byte[len];

        buf.get(bulkStr, 0, len);

        if (buf.get() != CR || buf.get() != LF)
            throw new IgniteCheckedException("Invalid request syntax!");

        return new String(bulkStr);
    }

    /**
     * @param buf
     * @return Count of elements.
     */
    private int elCnt(ByteBuffer buf) throws IgniteCheckedException {
        byte[] arrLen = new byte[9];

        int idx = 0;
        byte b = buf.get();
        while (b != CR) {
            arrLen[idx++] = b;
            b = buf.get();
        }

        if (buf.get() != LF)
            throw new IgniteCheckedException("Invalid request syntax!");

        return Integer.parseInt(new String(arrLen, 0, idx));
    }

    /**
     * Converts a simple string data to a {@link ByteBuffer}.
     *
     * @param str
     * @return
     */
    public static ByteBuffer toSimpleString(String str) {
        byte[] b = str.getBytes();

        ByteBuffer buf = ByteBuffer.allocate(b.length + 3);
        buf.put(SIMPLE_STRING);
        buf.put(b);
        buf.put(CRLF);

        buf.flip();

        return buf;
    }
}
