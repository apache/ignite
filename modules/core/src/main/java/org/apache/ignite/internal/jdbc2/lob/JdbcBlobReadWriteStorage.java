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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStream.MAX_ARRAY_SIZE;

/**
 * Read-write implementation of {@link JdbcBlobStorage}.
 *
 * <p>Keeps data in list of byte array buffers (of different size) to avoid memory
 * reallocation and data coping on write operations (which append data in particular).
 */
class JdbcBlobReadWriteStorage extends JdbcBlobStorage {
    /** Max capacity when it is still reasonable to double size for new buffer. */
    private static final int MAX_CAP = 32 * 1024 * 1024;

    /** The list of buffers. */
    private List<byte[]> buffers = new ArrayList<>();

    /**
     * Creates a new empty buffer.
     */
    JdbcBlobReadWriteStorage() {
        // No-op
    }

    /**
     * Creates a new buffer enclosing data from the existing byte array.
     *
     * @param arr The byte array.
     */
    JdbcBlobReadWriteStorage(byte[] arr) {
        if (arr.length > 0) {
            buffers.add(arr);

            totalCnt = arr.length;
        }
    }

    /** {@inheritDoc} */
    @Override int totalCnt() {
        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override int read(JdbcBlobBufferPointer pos) {
        byte[] buf = getBuf(pos);

        if (buf == null || pos.getPos() >= totalCnt)
            return -1;

        int res = buf[getBufPos(pos)] & 0xff;

        doAdvancePointer(pos, 1);

        return res;
    }

    /** {@inheritDoc} */
    @Override int read(JdbcBlobBufferPointer pos, byte[] res, int off, int cnt) {
        byte[] buf = getBuf(pos);

        if (buf == null || pos.getPos() >= totalCnt)
            return -1;

        int remaining = cnt;

        while (remaining > 0 && pos.getPos() < totalCnt && buf != null) {
            int toCopy = Math.min(remaining, buf.length - getBufPos(pos));

            if (toCopy > totalCnt - pos.getPos())
                toCopy = totalCnt - pos.getPos();

            U.arrayCopy(buf, getBufPos(pos), res, off + (cnt - remaining), toCopy);

            remaining -= toCopy;

            doAdvancePointer(pos, toCopy);

            buf = getBuf(pos);
        }

        return cnt - remaining;
    }

    /** {@inheritDoc} */
    @Override void write(JdbcBlobBufferPointer pos, int b) throws IOException {
        if (pos.getPos() > totalCnt)
            throw new IOException("Writting beyond end of Blob, it probably was truncated after OutputStream was created " +
                    "[pos=" + pos.getPos() + ", totalCnt=" + totalCnt + "]");

        if (MAX_ARRAY_SIZE - pos.getPos() < 1)
            throw new IOException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes to Blob.");

        byte[] buf = getBuf(pos);

        if (buf == null)
            buf = addNewBuffer(1);

        buf[getBufPos(pos)] = (byte)(b & 0xff);

        doAdvancePointer(pos, 1);

        totalCnt = Math.max(pos.getPos(), totalCnt);
    }

    /** {@inheritDoc} */
    @Override void write(JdbcBlobBufferPointer pos, byte[] bytes, int off, int len) throws IOException {
        if (pos.getPos() > totalCnt)
            throw new IOException("Writting beyond end of Blob, it probably was truncated after OutputStream was created " +
                    "[pos=" + pos.getPos() + ", totalCnt=" + totalCnt + "]");

        if (MAX_ARRAY_SIZE - pos.getPos() < len)
            throw new IOException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes to Blob.");

        int remaining = len;

        byte[] buf;

        while (remaining > 0 && (buf = getBuf(pos)) != null) {
            int toCopy = Math.min(remaining, buf.length - getBufPos(pos));

            U.arrayCopy(bytes, off + len - remaining, buf, getBufPos(pos), toCopy);

            remaining -= toCopy;

            doAdvancePointer(pos, toCopy);
        }

        if (remaining > 0) {
            addNewBuffer(remaining);

            U.arrayCopy(bytes, off + len - remaining, getBuf(pos), 0, remaining);

            doAdvancePointer(pos, remaining);
        }

        totalCnt = Math.max(pos.getPos(), totalCnt);
    }

    /** {@inheritDoc} */
    @Override int advancePointer(JdbcBlobBufferPointer pos, int step) {
        int toAdvance = Math.min(step, totalCnt - pos.getPos());

        if (toAdvance > 0)
            doAdvancePointer(pos, toAdvance);

        return toAdvance;
    }

    /** {@inheritDoc} */
    @Override void truncate(int len) {
        JdbcBlobBufferPointer pos = createPointer();

        advancePointer(pos, len);

        if (buffers.size() > getBufIdx(pos) + 1)
            buffers.subList(getBufIdx(pos) + 1, buffers.size()).clear();

        totalCnt = len;
    }

    /** {@inheritDoc} */
    @Override void close() {
        buffers.clear();
        buffers = null;
    }

    /**
     * Makes a new buffer available.
     *
     * @param neededBytes count of the additional bytes needed.
     * @return The new buffer.
     */
    private byte[] addNewBuffer(int neededBytes) {
        int newBufSize;

        if (buffers.isEmpty()) {
            newBufSize = neededBytes;
        }
        else if (buffers.get(buffers.size() - 1).length > MAX_CAP) {
            newBufSize = neededBytes + MAX_CAP;
        }
        else {
            newBufSize = Math.max(
                    buffers.get(buffers.size() - 1).length << 1,
                    neededBytes);
        }

        byte[] res = new byte[newBufSize];

        buffers.add(res);

        return res;
    }

    /**
     * Get buffer containing data at the given position.
     *
     * @param pos Position pointer.
     */
    private byte[] getBuf(JdbcBlobBufferPointer pos) {
        return getBufIdx(pos) < buffers.size() ? buffers.get(getBufIdx(pos)) : null;
    }

    /**
     * Extracts the current position in the current buffer.
     *
     * @param pos Position pointer.
     */
    private int getBufPos(JdbcBlobBufferPointer pos) {
        if (pos.getInBufPos() == null)
            recoverContext(pos);

        return pos.getInBufPos();
    }

    /**
     * Extracts index of the current buffer.
     *
     * @param pos Position pointer.
     */
    private int getBufIdx(JdbcBlobBufferPointer pos) {
        if (pos.getIdx() == null)
            recoverContext(pos);

        return pos.getIdx();
    }

    /**
     * Adds context to the {@code pointer}.
     *
     * <p>Calculates the current position in the current buffer taking into account
     * the current position stored in pointer.
     *
     * @param pointer Pointer.
     */
    private void recoverContext(JdbcBlobBufferPointer pointer) {
        int pos = pointer.getPos();

        pointer.set(0, 0, 0);

        doAdvancePointer(pointer, pos);
    }

    /**
     * Internal implementation of a position pointer forward movement.
     * Doesn't check the current totalCnt.
     *
     * @param pos Pointer to modify.
     * @param step Number of bytes to skip forward.
     */
    private void doAdvancePointer(JdbcBlobBufferPointer pos, int step) {
        int inBufPos = getBufPos(pos);
        int idx = getBufIdx(pos);
        int remain = step;

        while (remain > 0) {
            if (remain >= buffers.get(idx).length - inBufPos) {
                remain -= buffers.get(idx).length - inBufPos;

                inBufPos = 0;

                idx++;
            }
            else {
                inBufPos += remain;

                remain = 0;
            }
        }

        pos.set(pos.getPos() + step, idx, inBufPos);
    }
}
