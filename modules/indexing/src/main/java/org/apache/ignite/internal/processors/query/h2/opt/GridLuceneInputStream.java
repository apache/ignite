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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.io.EOFException;
import java.io.IOException;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;

/**
 * A memory-resident {@link IndexInput} implementation.
 */
public class GridLuceneInputStream extends IndexInput implements Cloneable {
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

    /** */
    private volatile boolean closed;

    /** */
    private boolean isClone;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param f File.
     * @throws IOException If failed.
     */
    public GridLuceneInputStream(String name, GridLuceneFile f) throws IOException {
        this(name, f, f.getLength());
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param f File.
     * @param length inputStream length.
     * @throws IOException If failed.
     */
    public GridLuceneInputStream(String name, GridLuceneFile f, final long length) throws IOException {
        super("RAMInputStream(name=" + name + ")");

        file = f;

        this.length = length;

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
        if (!isClone) {
            closed = true;

            file.releaseRef();
        }
    }

    /** {@inheritDoc} */
    @Override public IndexInput clone() {
        GridLuceneInputStream clone = (GridLuceneInputStream) super.clone();

        if (closed)
            throw new AlreadyClosedException(toString());

        clone.isClone = true;

        return clone;

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
    @Override public IndexInput slice(final String sliceDescription, final long offset, final long length)
        throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length)
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);

        final String newResourceDescription = (sliceDescription == null) ? toString() : (toString() + " [slice=" + sliceDescription + "]");

        return new SlicedInputStream(newResourceDescription, offset, length);
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

    /** */
    private class SlicedInputStream extends GridLuceneInputStream {
        /** */
        private final long offset;

        /** */
        public SlicedInputStream(String newResourceDescription, long offset, long length) throws IOException {
            super(newResourceDescription, GridLuceneInputStream.this.file, offset + length);

            // Avoid parent resource closing together with this.
            super.isClone = true;

            this.offset = offset;

            seek(0L);
        }

        /** {@inheritDoc} */
        @Override public void seek(long pos) throws IOException {
            if (pos < 0L) {
                throw new IllegalArgumentException("Seeking to negative position: " + this);
            }
            super.seek(pos + offset);
        }

        /** {@inheritDoc} */
        @Override public long getFilePointer() {
            return super.getFilePointer() - offset;
        }

        /** {@inheritDoc} */
        @Override public long length() {
            return super.length() - offset;
        }

        /** {@inheritDoc} */
        @Override public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
            return super.slice(sliceDescription, offset + ofs, len);
        }
    }
}
