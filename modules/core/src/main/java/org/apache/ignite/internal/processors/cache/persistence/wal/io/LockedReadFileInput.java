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

package org.apache.ignite.internal.processors.cache.persistence.wal.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.jetbrains.annotations.NotNull;

/**
 * File input, backed by byte buffer file input. This class allows to read data by chunks from file and then read
 * primitives.
 *
 * This implementation locks segment only for reading to buffer and also can switch reading segment from work directory
 * to archive directory if needed.
 */
final class LockedReadFileInput implements FileInput {
    /** Actual reader of file. */
    private FileInput delegate;
    /** Segment for read. */
    private final long segmentId;
    /** Holder of actual information of latest manipulation on WAL segments. */
    private final SegmentAware segmentAware;
    /** Factory of file I/O for segment. */
    private final SegmentIoFactory fileIOFactory;
    /** Last read was from archive or not. */
    private boolean isLastReadFromArchive;
    /** Buffer for reading blocks of data into. */
    private ByteBufferExpander expBuf;

    /**
     * @param buf Buffer for reading blocks of data into.
     * @param initFileIo Initial File I/O for reading.
     * @param segmentId Segment for read.
     * @param segmentAware Holder of actual information of latest manipulation on WAL segments.
     * @param segmentIOFactory Factory of file I/O for segment.
     * @throws IOException
     */
    LockedReadFileInput(
        ByteBufferExpander buf,
        FileIO initFileIo,
        long segmentId,
        SegmentAware segmentAware,
        SegmentIoFactory segmentIOFactory
    ) throws IOException {
        this.delegate = new SimpleFileInput(initFileIo, buf);
        expBuf = buf;
        this.segmentId = segmentId;
        this.segmentAware = segmentAware;
        this.fileIOFactory = segmentIOFactory;
        isLastReadFromArchive = segmentAware.lastArchivedAbsoluteIndex() >= segmentId;
    }

    /** {@inheritDoc} */
    public FileIO io() {
        return delegate.io();
    }

    /** {@inheritDoc} */
    public void seek(long pos) throws IOException {
        delegate.seek(pos);
    }

    /** {@inheritDoc} */
    public ByteBuffer buffer() {
        return delegate.buffer();
    }

    /** {@inheritDoc} */
    public void ensure(int requested) throws IOException {
        int available = buffer().remaining();

        if (available >= requested)
            return;

        boolean readArchive = segmentAware.checkCanReadArchiveOrReserveWorkSegment(segmentId);
        try {
            if (readArchive && !isLastReadFromArchive) {
                isLastReadFromArchive = true;

                FileIO io = fileIOFactory.build(segmentId);

                io.position(delegate.io().position());

                SimpleFileInput input = new SimpleFileInput(io, expBuf);

                this.delegate.io().close();

                this.delegate = input;
            }

            delegate.ensure(requested);
        }
        finally {
            if (!readArchive)
                segmentAware.releaseWorkSegment(segmentId);
        }
    }

    /** {@inheritDoc} */
    public long position() {
        return delegate.position();
    }

    /** {@inheritDoc} */
    public void readFully(@NotNull byte[] b) throws IOException {
        delegate.readFully(b);
    }

    /** {@inheritDoc} */
    public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
        delegate.readFully(b, off, len);
    }

    /** {@inheritDoc} */
    public int skipBytes(int n) throws IOException {
        return delegate.skipBytes(n);
    }

    /** {@inheritDoc} */
    public boolean readBoolean() throws IOException {
        return delegate.readBoolean();
    }

    /** {@inheritDoc} */
    public byte readByte() throws IOException {
        return delegate.readByte();
    }

    /** {@inheritDoc} */
    public int readUnsignedByte() throws IOException {
        return delegate.readUnsignedByte();
    }

    /** {@inheritDoc} */
    public short readShort() throws IOException {
        return delegate.readShort();
    }

    /** {@inheritDoc} */
    public int readUnsignedShort() throws IOException {
        return delegate.readUnsignedShort();
    }

    /** {@inheritDoc} */
    public char readChar() throws IOException {
        return delegate.readChar();
    }

    /** {@inheritDoc} */
    public int readInt() throws IOException {
        return delegate.readInt();
    }

    /** {@inheritDoc} */
    public long readLong() throws IOException {
        return delegate.readLong();
    }

    /** {@inheritDoc} */
    public float readFloat() throws IOException {
        return delegate.readFloat();
    }

    /** {@inheritDoc} */
    public double readDouble() throws IOException {
        return delegate.readDouble();
    }

    /** {@inheritDoc} */
    public String readLine() throws IOException {
        return delegate.readLine();
    }

    /** {@inheritDoc} */
    public String readUTF() throws IOException {
        return delegate.readUTF();
    }


    /** {@inheritDoc} */
    @NotNull
    public Crc32CheckingFileInput startRead(boolean skipCheck) {
        return delegate.startRead(skipCheck);
    }

    /**
     * Resolving fileIo for segment.
     */
    interface SegmentIoFactory {
        /**
         * @param segmentId Segment for IO action.
         * @return {@link FileIO}.
         * @throws IOException
         */
        FileIO build(long segmentId) throws IOException;
    }
}
