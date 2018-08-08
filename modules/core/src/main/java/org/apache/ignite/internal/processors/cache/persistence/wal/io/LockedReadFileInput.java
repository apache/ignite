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
final class LockedReadFileInput extends SimpleFileInput {
    /** Segment for read. */
    private final long segmentId;
    /** Holder of actual information of latest manipulation on WAL segments. */
    private final SegmentAware segmentAware;
    /** Factory of file I/O for segment. */
    private final SegmentIoFactory fileIOFactory;
    /** Last read was from archive or not. */
    private boolean isLastReadFromArchive;

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
        super(initFileIo, buf);
        this.segmentId = segmentId;
        this.segmentAware = segmentAware;
        this.fileIOFactory = segmentIOFactory;
        isLastReadFromArchive = segmentAware.lastArchivedAbsoluteIndex() >= segmentId;
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

                io.position(io().position());

                io().close();

                this.io = io;
            }

            super.ensure(requested);
        }
        finally {
            if (!readArchive)
                segmentAware.releaseWorkSegment(segmentId);
        }
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
