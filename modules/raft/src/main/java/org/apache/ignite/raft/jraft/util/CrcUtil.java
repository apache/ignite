/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.nio.ByteBuffer;

/**
 * CRC utilities to compute CRC64 checksum.
 */
public final class CrcUtil {

    private static final ThreadLocal<CRC64> CRC_64_THREAD_LOCAL = ThreadLocal.withInitial(CRC64::new);

    /**
     * Compute CRC64 checksum for byte[].
     *
     * @param array source array
     * @return checksum value
     */
    public static long crc64(final byte[] array) {
        if (array == null) {
            return 0;
        }
        return crc64(array, 0, array.length);
    }

    /**
     * Compute CRC64 checksum for byte[].
     *
     * @param array source array
     * @param offset starting position in the source array
     * @param length the number of array elements to be computed
     * @return checksum value
     */
    public static long crc64(final byte[] array, final int offset, final int length) {
        final CRC64 crc64 = CRC_64_THREAD_LOCAL.get();
        crc64.update(array, offset, length);
        final long ret = crc64.getValue();
        crc64.reset();
        return ret;
    }

    /**
     * Compute CRC64 checksum for {@code ByteBuffer}.
     *
     * @param buf source {@code ByteBuffer}
     * @return checksum value
     */
    public static long crc64(final ByteBuffer buf) {
        final int pos = buf.position();
        final int rem = buf.remaining();
        if (rem <= 0) {
            return 0;
        }
        // Currently we have not used DirectByteBuffer yet.
        if (buf.hasArray()) {
            return crc64(buf.array(), pos + buf.arrayOffset(), rem);
        }
        final byte[] b = new byte[rem];
        buf.mark();
        buf.get(b);
        buf.reset();
        return crc64(b);
    }

    private CrcUtil() {
    }
}