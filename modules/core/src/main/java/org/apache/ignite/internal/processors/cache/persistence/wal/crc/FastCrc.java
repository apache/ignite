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

package org.apache.ignite.internal.processors.cache.persistence.wal.crc;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 */
public final class FastCrc {
    /** CRC algo. */
    private static final ThreadLocal<CRC32> CRC = ThreadLocal.withInitial(CRC32::new);

    /** */
    private final CRC32 crc = new CRC32();

    /**
     * Current value.
     */
    private int val;

    public FastCrc() {
        reset();
    }

    /**
     * preparation for further calculations
     */
    public void reset() {
        val = 0xffffffff;

        crc.reset();
    }

    /**
     * return crc value.
     */
    public int getValue() {
        return val;
    }

    /**
     * @param b B.
     * @param len Length.
     */
    public void update(final ByteBuffer b, final int len) {
        val = calcCrc(crc, b, len);
    }

    /**
     * @param buf Input buffer.
     * @param len Buffer length.
     *
     * @return Crc checksum.
     */
    public static int calcCrc(ByteBuffer buf, int len) {
        CRC32 crcAlgo = CRC.get();

        int res = calcCrc(crcAlgo, buf, len);

        crcAlgo.reset();

        return res;
    }

    /**
     * @param crcAlgo CRC algorithm.
     * @param buf Input buffer.
     * @param len Buffer length.
     * @param reset Reset crc flag.
     *
     * @return Crc checksum.
     */
    private static int calcCrc(CRC32 crcAlgo, ByteBuffer buf, int len) {
        int pos = buf.position();

        int res;

        if (buf.capacity() <= pos + len)
            crcAlgo.update(buf);
        else if (buf.hasArray()) {
            crcAlgo.update(buf.array(), pos, len);

            buf.position(pos + len);
        }
        else {
            byte[] buf0 = new byte[len];

            buf.get(buf0, 0, len);

            crcAlgo.update(buf0);
        }

        res = (int)crcAlgo.getValue() ^ 0xFFFFFFFF;

        return res;
    }
}
