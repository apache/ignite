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

package org.apache.ignite.internal.jdbc2.lob;

import java.io.IOException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * In-memory based implementation of the {@link JdbcBlobStorage} which wraps
 * a part of an externally provided byte array which can not be modified.
 *
 * <p>Says, allows direct read-only access to binary data stored in the incoming JDBC
 * responce message without copying.
 */
class JdbcBlobReadOnlyMemoryStorage implements JdbcBlobStorage {
    /** External buffer.*/
    private final byte[] buf;

    /** Offset to the first byte to be wrapped.*/
    private final int off;

    /** The length in bytes of the data to be wrapped.*/
    private final long totalCnt;

    /**
     * Constructor.
     *
     * @param buf External buffer.
     * @param off The offset to the first byte to be wrapped.
     * @param len The length in bytes of the data to be wrapped.
     */
    JdbcBlobReadOnlyMemoryStorage(byte[] buf, int off, int len) {
        this.buf = buf;
        this.off = off;
        totalCnt = len;
    }

    /** {@inheritDoc} */
    @Override public long totalCnt() {
        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override public JdbcBlobBufferPointer createPointer() {
        return new JdbcBlobBufferPointer();
    }

    /** {@inheritDoc} */
    @Override public int read(JdbcBlobBufferPointer pos) throws IOException {
        if (pos.getPos() >= totalCnt)
            return -1;

        int res = buf[Math.toIntExact(pos.getPos() + off)] & 0xff;

        advance(pos, 1);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int read(JdbcBlobBufferPointer pos, byte[] res, int off, int cnt) throws IOException {
        if (pos.getPos() >= totalCnt + off)
            return -1;

        int idx = Math.toIntExact(pos.getPos() + this.off);

        int size = cnt > totalCnt + this.off - idx ? Math.toIntExact(totalCnt + this.off - idx) : cnt;

        U.arrayCopy(buf, idx, res, off, size);

        advance(pos, size);

        return size;
    }

    /** {@inheritDoc} */
    @Override public void write(JdbcBlobBufferPointer pos, int b) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void write(JdbcBlobBufferPointer pos, byte[] bytes, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void advance(JdbcBlobBufferPointer pos, long step) {
        pos.setPos(pos.getPos() + step);
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }
}
