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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.segment.SegmentAware;
import org.jetbrains.annotations.NotNull;

/**
 * File input, backed by byte buffer file input. This class allows to read data by chunks from file and then read
 * primitives.
 *
 * This implementation locks segment only for reading to buffer and also can switch reading segment from work directory
 * to archive directory if needed.
 */
public final class LockedReadFileInput implements FileInput {

    private FileInput input;
    private final long segmentId;
    private final SegmentAware segmentAware;
    private final FileIoResolver fileIOFactory;
    private boolean readFromArchive;
    /** */
    private ByteBufferExpander expBuf;

    public LockedReadFileInput(ByteBufferExpander buf, FileIO initFileIo, long segmentId, SegmentAware segmentAware,
        FileIoResolver fileIOFactory) throws IOException {
        this.input = new SimpleFileInput(initFileIo, buf);
        expBuf = buf;
        this.segmentId = segmentId;
        this.segmentAware = segmentAware;
        this.fileIOFactory = fileIOFactory;
        readFromArchive = segmentAware.lastArchivedAbsoluteIndex() >= segmentId;
    }

    public FileIO io() {
        return input.io();
    }

    public void seek(long pos) throws IOException {
        input.seek(pos);
    }

    public ByteBuffer buffer() {
        return input.buffer();
    }

    public void ensure(int requested) throws IOException {
        int available = buffer().remaining();

        if (available >= requested)
            return;

        boolean readArchive = segmentAware.checkCanReadArchiveOrReserveWorkSegment(segmentId);
        try {

            if (readArchive && !readFromArchive) {
                readFromArchive = true;

                FileIO io = fileIOFactory.resolve(segmentId);

                io.position(input.io().position());

                SimpleFileInput input = new SimpleFileInput(io, expBuf);

                this.input.io().close();

                this.input = input;
            }

            input.ensure(requested);
        }
        finally {
            if (!readArchive)
                segmentAware.releaseWorkSegment(segmentId);
        }
    }

    public long position() {
        return input.position();
    }

    public void readFully(@NotNull byte[] b) throws IOException {
        input.readFully(b);
    }

    public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    public byte readByte() throws IOException {
        return input.readByte();
    }

    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    public short readShort() throws IOException {
        return input.readShort();
    }

    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    public char readChar() throws IOException {
        return input.readChar();
    }

    public int readInt() throws IOException {
        return input.readInt();
    }

    public long readLong() throws IOException {
        return input.readLong();
    }

    public float readFloat() throws IOException {
        return input.readFloat();
    }

    public double readDouble() throws IOException {
        return input.readDouble();
    }

    public String readLine() throws IOException {
        return input.readLine();
    }

    public String readUTF() throws IOException {
        return input.readUTF();
    }

    @NotNull
    public Crc32CheckingFileInput startRead(boolean skipCheck) {
        return input.startRead(skipCheck);
    }

    /**
     * Resolving fileIo for segment.
     */
    interface FileIoResolver {
        /**
         * @param segmentId Segment for IO action.
         * @return {@link FileIO}.
         * @throws IOException
         */
        FileIO resolve(long segmentId) throws IOException;
    }
}
