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

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Read-only implementation of the {@link JdbcBlobStorage} which wraps
 * a part of an externally provided byte array which can not be modified.
 *
 * <p>Say, allows direct read-only access to binary data stored in the incoming JDBC
 * response message without copying.
 */
class JdbcBlobReadOnlyStorage extends JdbcBlobStorage {
    /** External buffer.*/
    private final byte[] buf;

    /** Offset to the first byte to be wrapped.*/
    private final int off;

    /**
     * Constructor.
     *
     * @param buf External buffer.
     * @param off The offset to the first byte to be wrapped.
     * @param len The length in bytes of the data to be wrapped.
     */
    JdbcBlobReadOnlyStorage(byte[] buf, int off, int len) {
        this.buf = buf;
        this.off = off;
        totalCnt = len;
    }

    /** {@inheritDoc} */
    @Override int read(JdbcBlobBufferPointer pos) {
        if (pos.getPos() >= totalCnt)
            return -1;

        int res = buf[pos.getPos() + off] & 0xff;

        doAdvance(pos, 1);

        return res;
    }

    /** {@inheritDoc} */
    @Override int read(JdbcBlobBufferPointer pos, byte[] resBuf, int resOff, int cnt) {
        if (pos.getPos() >= totalCnt)
            return -1;

        int bufOff = pos.getPos() + off;

        int size = Math.min(cnt, totalCnt - pos.getPos());

        U.arrayCopy(buf, bufOff, resBuf, resOff, size);

        doAdvance(pos, size);

        return size;
    }

    /** {@inheritDoc} */
    @Override void write(JdbcBlobBufferPointer pos, int b) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override void write(JdbcBlobBufferPointer pos, byte[] bytes, int off, int len) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override void truncate(int len) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override int advancePointer(JdbcBlobBufferPointer pos, int step) {
        int toAdvance = Math.min(step, totalCnt - pos.getPos());

        if (toAdvance > 0)
            doAdvance(pos, toAdvance);

        return toAdvance;
    }

    /**
     * Internal implementation of a position pointer movement.
     * Doesn't check the current totalCnt.
     *
     * @param pos Pointer to modify.
     * @param step Number of bytes to skip forward.
     */
    private void doAdvance(JdbcBlobBufferPointer pos, int step) {
        pos.setPos(pos.getPos() + step);
    }
}
