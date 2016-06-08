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

package org.apache.ignite.internal.util.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class defines output stream backed by byte array.
 * It is identical to {@link ByteArrayOutputStream} with no synchronization.
 */
public class GridByteArrayOutputStream extends OutputStream {
    /** The buffer where data is stored. */
    private byte buf[];

    /** The number of valid bytes in the buffer. */
    private int cnt;

    /**
     * Creates a new byte array output stream. The buffer capacity is
     * initially 32 bytes, though its size increases if necessary.
     */
    public GridByteArrayOutputStream() {
        this(32);
    }

    /**
     * @param size Byte array size.
     */
    public GridByteArrayOutputStream(int size) {
        this(size, 0);
    }

    /**
     * @param size Byte array size.
     * @param off Offset from which to write.
     */
    public GridByteArrayOutputStream(int size, int off) {
        if (size < 0)
            throw new IllegalArgumentException("Negative initial size: " + size);

        if (off > size)
            throw new IllegalArgumentException("Invalid offset: " + off);

        buf = new byte[size];

        cnt = off;
    }

    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param b the byte to be written.
     */
    @Override public void write(int b) {
        int newCnt = cnt + 1;

        if (newCnt > buf.length)
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newCnt));

        buf[cnt] = (byte) b;

        cnt = newCnt;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this byte array output stream.
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    @Override public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0))
            throw new IndexOutOfBoundsException();
        else if (len == 0)
            return;

        int newCnt = cnt + len;

        if (newCnt > buf.length)
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newCnt));

        U.arrayCopy(b, off, buf, cnt, len);

        cnt = newCnt;
    }

    /**
     * Writes the complete contents of this byte array output stream to
     * the specified output stream argument, as if by calling the output
     * stream's write method using <code>out.write(buf, 0, count)</code>.
     *
     * @param out the output stream to which to write the data.
     * @throws IOException if an I/O error occurs.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, cnt);
    }

    /**
     * Resets the <code>count</code> field of this byte array output
     * stream to zero, so that all currently accumulated output in the
     * output stream is discarded. The output stream can be used again,
     * reusing the already allocated buffer space.
     *
     * @see ByteArrayInputStream#count
     */
    public void reset() {
        cnt = 0;
    }

    /**
     * Returns internal array without copy (use with care).
     *
     * @return Internal array without copy.
     */
    public byte[] internalArray() {
        return buf;
    }

    /**
     * Creates a newly allocated byte array. Its size is the current
     * size of this output stream and the valid contents of the buffer
     * have been copied into it.
     *
     * @return the current contents of this output stream, as a byte array.
     * @see ByteArrayOutputStream#size()
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(buf, cnt);
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number
     *         of valid bytes in this output stream.
     * @see ByteArrayOutputStream#count
     */
    public int size() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridByteArrayOutputStream.class, this);
    }
}