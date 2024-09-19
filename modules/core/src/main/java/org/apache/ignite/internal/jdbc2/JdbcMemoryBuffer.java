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

package org.apache.ignite.internal.jdbc2;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;

public class JdbcMemoryBuffer {
    /** The list of buffers, which grows and never reduces. */
    protected List<byte[]> buffers;

    /** The total count of bytes. */
    protected long totalCnt;

    /** */
    public JdbcMemoryBuffer() {
        buffers = new ArrayList<>();

        totalCnt = 0;
    }

    /** */
    public long getLength() {
        return totalCnt;
    }

    /** */
    public OutputStream getOutputStream() {
        return new BufferOutputStream(0);
    }

    /** */
    public OutputStream getOutputStream(long pos) {
        return new BufferOutputStream(pos);
    }

    /** */
    public InputStream getInputStream() {
        if (buffers.isEmpty() || totalCnt == 0)
            return InputStream.nullInputStream();

        return new BufferInputStream(0, totalCnt);
    }

    /**
     * @param pos the offset to the first byte of the partial value to be
     *        retrieved. The first byte in the {@code Blob} is at position 0.
     * @param len the length in bytes of the partial value to be retrieved
     * @return {@code InputStream} through which
     *         the partial {@code Blob} value can be read.
     */
    public InputStream getInputStream(long pos, long len) {
        if (pos < 0 || len < 0 || pos > totalCnt)
            throw new RuntimeException("Invalid argument. Position can't be less than 0 or " +
                    "greater than size of underlying memory buffers. Requested length can't be negative and can't be " +
                    "greater than available bytes from given position [pos=" + pos + ", len=" + len + ']');

        if (buffers.isEmpty() || totalCnt == 0 || len == 0 || pos == totalCnt)
            return InputStream.nullInputStream();

        return new BufferInputStream(pos, len);
    }

    /**
     * Makes a new buffer available
     *
     * @param newCount the new size of the Blob
     */
    protected void addNewBuffer(final int newCount) {
        final int newBufSize;

        if (buffers.isEmpty()) {
            newBufSize = newCount;
        }
        else {
            newBufSize = Math.max(
                    buffers.get(buffers.size() - 1).length << 1,
                    (newCount));
        }

        buffers.add(new byte[newBufSize]);
    }

    /**
     *
     */
    private class BufferInputStream extends InputStream {
        /** The index of the current buffer. */
        private int bufIdx;

        /** Current position in the current buffer. */
        private int inBufPos;

        /** Global current position. */
        private long pos;

        /** Starting position. */
        private final long start;

        /** Stream length. */
        private final long len;

        /**
         * @param start starting position.
         */
        BufferInputStream(long start, long len) {
            bufIdx = 0;

            this.start = pos = start;

            this.len = len;

            for (long p = 0; p < totalCnt;) {
                if (start > p + buffers.get(bufIdx).length - 1) {
                    p += buffers.get(bufIdx++).length;
                }
                else {
                    inBufPos = (int)(start - p);
                    break;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public int read() {
            if (pos >= start + len || pos >= totalCnt)
                return -1;

            int res = buffers.get(bufIdx)[inBufPos] & 0xff;

            inBufPos++;
            pos++;

            if (pos < start + len) {
                if (inBufPos == buffers.get(bufIdx).length) {
                    bufIdx++;

                    inBufPos = 0;
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public int read(byte res[], int off, int cnt) {
            if (pos >= start + len || pos >= totalCnt)
                return -1;

            long availableBytes = Math.min(start + len, totalCnt) - pos;

            int size = cnt < availableBytes ? cnt : (int)availableBytes;

            int remaining = size;

            while (remaining > 0 && bufIdx < buffers.size()) {
                byte[] buf = buffers.get(bufIdx);

                int toCopy = Math.min(remaining, buf.length - inBufPos);

                U.arrayCopy(buf, Math.max(inBufPos, 0), res, off + (size - remaining), toCopy);

                remaining -= toCopy;

                pos += toCopy;
                inBufPos += toCopy;

                if (inBufPos == buffers.get(bufIdx).length) {
                    inBufPos = 0;

                    bufIdx++;
                }
            }

            return size;
        }
    }

    /** */
    private class BufferOutputStream extends OutputStream {
        /** The index of the current buffer. */
        private int bufIdx;

        /** Position in the current buffer. */
        private int inBufPos;

        /** Global current position. */
        private long pos;

        /**
         * @param pos starting position.
         */
        BufferOutputStream(long pos) {
            bufIdx = 0;

            this.pos = pos;

            for (long p = 0; p < totalCnt;) {
                if (pos > p + buffers.get(bufIdx).length - 1) {
                    p += buffers.get(bufIdx++).length;
                }
                else {
                    inBufPos = (int)(pos - p);
                    break;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void write(int b) {
            write(new byte[] {(byte)b}, 0, 1);
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] bytes, int off, int len) {
            int remaining = len;

            for (; bufIdx < buffers.size(); bufIdx++) {
                byte[] buf = buffers.get(bufIdx);

                int toCopy = Math.min(remaining, buf.length - inBufPos);

                U.arrayCopy(bytes, off + len - remaining, buf, inBufPos, toCopy);

                remaining -= toCopy;

                if (remaining == 0) {
                    inBufPos += toCopy;

                    break;
                }
                else {
                    inBufPos = 0;
                }
            }

            if (remaining > 0) {
                addNewBuffer(remaining);

                U.arrayCopy(bytes, off + len - remaining, buffers.get(buffers.size() - 1), 0, remaining);

                bufIdx = buffers.size() - 1;
                inBufPos = remaining;
            }

            totalCnt = Math.max(pos + len, totalCnt);

            pos += len;
        }
    }
}
