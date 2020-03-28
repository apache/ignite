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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

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
     * @param file A file to calculate checksum over it.
     * @return CRC32 checksum.
     * @throws IOException If fails.
     */
    public static int calcCrc(File file) throws IOException {
        assert !file.isDirectory() : "CRC32 can't be calculated over directories";

        CRC32 algo = new CRC32();

        try (InputStream in = new CheckedInputStream(new FileInputStream(file), algo)) {
            byte[] buf = new byte[1024];

            while (in.read(buf) != -1)
                ;
        }

        return ~(int)algo.getValue();
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

        return ~(int)crcAlgo.getValue();
    }
}
