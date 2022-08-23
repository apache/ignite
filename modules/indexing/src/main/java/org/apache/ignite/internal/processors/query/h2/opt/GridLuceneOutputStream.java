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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * A memory-resident {@link IndexOutput} implementation.
 */
public class GridLuceneOutputStream extends IndexOutput implements Accountable {
    /** Off-heap page size. */
    static final int BUFFER_SIZE = 32 * 1024;

    /** */
    private GridLuceneFile file;

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
    private final Checksum crc;

    /**
     * Constructor.
     *
     * @param f File.
     */
    public GridLuceneOutputStream(GridLuceneFile f) {
        super("RAMOutputStream(name=\"" + f.getName() + "\")", f.getName());

        file = f;

        mem = f.getDirectory().memory();

        // make sure that we switch to the
        // first needed buffer lazily
        currBufIdx = -1;
        currBuf = 0;

        crc = new BufferedChecksum(new CRC32());
    }

    /**
     * Resets this to an empty file.
     */
    public void reset() {
        currBuf = 0;
        currBufIdx = -1;
        bufPosition = 0;
        bufStart = 0;
        bufLength = 0;

        file.setLength(0);
        crc.reset();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        flush();

        file.releaseRef();
    }

    /** {@inheritDoc} */
    @Override public long getChecksum() throws IOException {
        return crc.getValue();
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte b) throws IOException {
        if (bufPosition == bufLength) {
            currBufIdx++;

            switchCurrentBuffer();
        }

        crc.update(b);

        mem.writeByte(currBuf + bufPosition++, b);
    }

    /** {@inheritDoc} */
    @Override public void writeBytes(byte[] b, int offset, int len) throws IOException {
        assert b != null;

        crc.update(b, offset, len);

        while (len > 0) {
            if (bufPosition == bufLength) {
                currBufIdx++;

                switchCurrentBuffer();
            }

            int remainInBuf = BUFFER_SIZE - bufPosition;
            int bytesToCp = len < remainInBuf ? len : remainInBuf;

            mem.writeBytes(currBuf + bufPosition, b, offset, bytesToCp);

            offset += bytesToCp;
            len -= bytesToCp;

            bufPosition += bytesToCp;
        }
    }

    /**
     * Switch buffer to next.
     */
    private void switchCurrentBuffer() {
        currBuf = currBufIdx == file.numBuffers() ? file.addBuffer() : file.getBuffer(currBufIdx);

        bufPosition = 0;
        bufStart = (long)BUFFER_SIZE * (long)currBufIdx;
        bufLength = BUFFER_SIZE;
    }

    /**
     * Sets file length.
     */
    private void setFileLength() {
        long pointer = bufStart + bufPosition;

        if (pointer > file.getLength())
            file.setLength(pointer);
    }

    /** Forces any buffered output to be written. */
    private void flush() throws IOException {
        setFileLength();
    }

    /** {@inheritDoc} */
    @Override public long getFilePointer() {
        return currBufIdx < 0 ? 0 : bufStart + bufPosition;
    }

    /** {@inheritDoc} */
    @Override public void copyBytes(DataInput input, long numBytes) throws IOException {
        assert numBytes >= 0 : "numBytes=" + numBytes;

        GridLuceneInputStream gridInput = input instanceof GridLuceneInputStream ? (GridLuceneInputStream)input : null;

        while (numBytes > 0) {
            if (bufPosition == bufLength) {
                currBufIdx++;

                switchCurrentBuffer();
            }

            int toCp = BUFFER_SIZE - bufPosition;

            if (numBytes < toCp)
                toCp = (int)numBytes;

            if (gridInput != null)
                gridInput.readBytes(currBuf + bufPosition, toCp);
            else {
                byte[] buff = new byte[toCp];

                input.readBytes(buff, 0, toCp, false);

                mem.writeBytes(currBuf + bufPosition, buff);
            }

            numBytes -= toCp;
            bufPosition += toCp;
        }
    }

    /** {@inheritDoc} */
    @Override public long ramBytesUsed() {
        return file.getSizeInBytes();
    }

    /** {@inheritDoc} */
    @Override public Collection<Accountable> getChildResources() {
        return Collections.singleton(Accountables.namedAccountable("file", file));
    }
}
