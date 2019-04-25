/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * This CRC calculation implementation workf much faster then {@link PureJavaCrc32}
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

    /** */
    public FastCrc() {
        reset();
    }

    /**
     * Preparation for further calculations.
     */
    public void reset() {
        val = 0xffffffff;

        crc.reset();
    }

    /**
     * @return crc value.
     */
    public int getValue() {
        return val;
    }

    /**
     * @param buf Input buffer.
     * @param len Data length.
     */
    public void update(final ByteBuffer buf, final int len) {
        val = calcCrc(crc, buf, len);
    }

    /**
     * @param buf Input buffer.
     * @param len Data length.
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
     *
     * @return Crc checksum.
     */
    private static int calcCrc(CRC32 crcAlgo, ByteBuffer buf, int len) {
        int initLimit = buf.limit();

        buf.limit(buf.position() + len);

        crcAlgo.update(buf);

        buf.limit(initLimit);

        return (int)crcAlgo.getValue() ^ 0xFFFFFFFF;
    }
}
