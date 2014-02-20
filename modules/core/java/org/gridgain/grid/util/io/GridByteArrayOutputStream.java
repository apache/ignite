// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.io;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * This class defines output stream backed by byte array.
 * It is identical to {@link java.io.ByteArrayOutputStream} with no synchronization.
 *
 * @author @java.author
 * @version @java.version
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
     * @see java.io.ByteArrayInputStream#count
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
     * @see java.io.ByteArrayOutputStream#size()
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(buf, cnt);
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number
     *         of valid bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridByteArrayOutputStream.class, this);
    }
}
