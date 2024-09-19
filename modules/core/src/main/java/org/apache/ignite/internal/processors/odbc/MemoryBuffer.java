package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MemoryBuffer {
    /** The list of buffers, which grows and never reduces. */
    private final List<byte[]> buffers;

    /** The total count of bytes. */
    protected long cnt;

    /**
     * Constructor.
     *
     * @param bufs The list of buffers.
     * @param cnt The total count of bytes in buffers.
     */
    public MemoryBuffer(List<byte[]> bufs, long cnt) {
        buffers = bufs;

        this.cnt = cnt;
    }

    /** */
    public MemoryBuffer() {
        buffers = new ArrayList<>();

        buffers.add(new byte[1024]);

        cnt = 0;
    }

    /** */
    public long getCnt() {
        return cnt;
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
        return getInputStreamImpl(0, cnt);
    }

    /** */
    public InputStream getInputStream(long pos, long len) {
        if (pos < 0 || len < 1 || pos >= cnt || len > cnt - pos)
            throw new RuntimeException("Invalid argument. Position can't be less than 0 or " +
                    "greater than size of underlying memory buffers. Requested length can't be negative and can't be " +
                    "greater than available bytes from given position [pos=" + pos + ", len=" + len + ']');

        return getInputStreamImpl(pos, len);
    }

    /**
     * @param pos the offset to the first byte of the partial value to be
     *        retrieved. The first byte in the {@code Blob} is at position 0.
     * @param len the length in bytes of the partial value to be retrieved
     * @return {@code InputStream} through which
     *         the partial {@code Blob} value can be read.
     */
    private InputStream getInputStreamImpl(long pos, long len) {
        long remaining = Math.min(len, cnt - pos);

        final List<ByteArrayInputStream> list = new ArrayList<>(buffers.size());

        long curPos = 0;

        for (byte[] buf : buffers) {
            if (pos < curPos + buf.length) {
                int toCopy = (int)Math.min(remaining, buf.length - Math.max(pos - curPos, 0));

                list.add(new ByteArrayInputStream(buf, (int)Math.max(pos - curPos, 0), toCopy));

                remaining -= toCopy;
            }

            curPos += buf.length;

            if (remaining == 0)
                break;
        }

        return new SequenceInputStream(Collections.enumeration(list));
    }

    /**
     * Makes a new buffer available
     *
     * @param newCount the new size of the Blob
     */
    private void addNewBuffer(final int newCount) {
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

            for (long p = 0; p < cnt;) {
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
        @Override public void write(int b) throws IOException {
            write(new byte[] {(byte)b}, 0, 1);
        }

        /** {@inheritDoc} */
        @Override public void write(byte b[], int off, int len) {
            int written = setBytesImpl0(b, off, len);

            cnt = Math.max(pos + written, cnt);

            pos += written;
        }

        /**
         */
        private int setBytesImpl0(byte[] bytes, int off, int len) {
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

            return len;
        }
    }
}
