/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.opt;

import org.apache.lucene.store.*;
import org.gridgain.grid.util.offheap.unsafe.*;

import java.io.*;

import static org.gridgain.grid.kernal.processors.query.h2.opt.GridLuceneOutputStream.*;

/**
 * A memory-resident {@link IndexInput} implementation.
 */
public class GridLuceneInputStream extends IndexInput {
    /** */
    private GridLuceneFile file;

    /** */
    private long length;

    /** */
    private long currBuf;

    /** */
    private int currBufIdx;

    /** */
    private int bufPosition;

    /** */
    private long bufStart;

    /** */
    private int bufLength;

    /** */
    private final GridUnsafeMemory mem;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param f File.
     * @throws IOException If failed.
     */
    public GridLuceneInputStream(String name, GridLuceneFile f) throws IOException {
        super("RAMInputStream(name=" + name + ")");

        file = f;

        length = file.getLength();

        if (length / BUFFER_SIZE >= Integer.MAX_VALUE)
            throw new IOException("RAMInputStream too large length=" + length + ": " + name);

        mem = file.getDirectory().memory();

        // make sure that we switch to the
        // first needed buffer lazily
        currBufIdx = -1;
        currBuf = 0;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // nothing to do here
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return length;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        if (bufPosition >= bufLength) {
            currBufIdx++;

            switchCurrentBuffer(true);
        }

        return mem.readByte(currBuf + bufPosition++);
    }

    /** {@inheritDoc} */
    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (bufPosition >= bufLength) {
                currBufIdx++;

                switchCurrentBuffer(true);
            }

            int remainInBuf = bufLength - bufPosition;
            int bytesToCp = len < remainInBuf ? len : remainInBuf;

            mem.readBytes(currBuf + bufPosition, b, offset, bytesToCp);

            offset += bytesToCp;
            len -= bytesToCp;

            bufPosition += bytesToCp;
        }
    }

    /**
     * Switch buffer to next.
     *
     * @param enforceEOF if we need to enforce {@link EOFException}.
     * @throws IOException if failed.
     */
    private void switchCurrentBuffer(boolean enforceEOF) throws IOException {
        bufStart = (long)BUFFER_SIZE * (long)currBufIdx;

        if (currBufIdx >= file.numBuffers()) {
            // end of file reached, no more buffers left
            if (enforceEOF)
                throw new EOFException("read past EOF: " + this);

            // Force EOF if a read takes place at this position
            currBufIdx--;
            bufPosition = BUFFER_SIZE;
        }
        else {
            currBuf = file.getBuffer(currBufIdx);
            bufPosition = 0;

            long buflen = length - bufStart;

            bufLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int)buflen;
        }
    }

    /** {@inheritDoc} */
    @Override public void copyBytes(IndexOutput out, long numBytes) throws IOException {
        assert numBytes >= 0 : "numBytes=" + numBytes;

        GridLuceneOutputStream gridOut = out instanceof GridLuceneOutputStream ? (GridLuceneOutputStream)out : null;

        long left = numBytes;

        while (left > 0) {
            if (bufPosition == bufLength) {
                ++currBufIdx;

                switchCurrentBuffer(true);
            }

            final int bytesInBuf = bufLength - bufPosition;
            final int toCp = (int)(bytesInBuf < left ? bytesInBuf : left);

            if (gridOut != null)
                gridOut.writeBytes(currBuf + bufPosition, toCp);
            else {
                byte[] buff = new byte[toCp];

                mem.readBytes(currBuf + bufPosition, buff);

                out.writeBytes(buff, toCp);
            }

            bufPosition += toCp;

            left -= toCp;
        }

        assert left == 0 : "Insufficient bytes to copy: numBytes=" + numBytes + " copied=" + (numBytes - left);
    }

    /**
     * For direct calls from {@link GridLuceneOutputStream}.
     *
     * @param ptr Pointer.
     * @param len Length.
     * @throws IOException If failed.
     */
    void readBytes(long ptr, int len) throws IOException {
        while (len > 0) {
            if (bufPosition >= bufLength) {
                currBufIdx++;

                switchCurrentBuffer(true);
            }

            int remainInBuf = bufLength - bufPosition;
            int bytesToCp = len < remainInBuf ? len : remainInBuf;

            mem.copyMemory(currBuf + bufPosition, ptr, bytesToCp);

            ptr += bytesToCp;
            len -= bytesToCp;

            bufPosition += bytesToCp;
        }
    }

    /** {@inheritDoc} */
    @Override public long getFilePointer() {
        return currBufIdx < 0 ? 0 : bufStart + bufPosition;
    }

    /** {@inheritDoc} */
    @Override public void seek(long pos) throws IOException {
        if (currBuf == 0 || pos < bufStart || pos >= bufStart + BUFFER_SIZE) {
            currBufIdx = (int)(pos / BUFFER_SIZE);

            switchCurrentBuffer(false);
        }

        bufPosition = (int)(pos % BUFFER_SIZE);
    }
}
