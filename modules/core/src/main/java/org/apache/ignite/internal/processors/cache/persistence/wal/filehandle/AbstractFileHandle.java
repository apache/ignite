/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.filehandle;

import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public abstract class AbstractFileHandle {
    /** I/O interface for read/write operations with file. */
    protected SegmentIO fileIO;

    /** Segment idx corresponded to fileIo. */
    private final long segmentIdx;

    /**
     * @param fileIO I/O interface for read/write operations of AbstractFileHandle.
     */
    public AbstractFileHandle(@NotNull SegmentIO fileIO) {
        this.fileIO = fileIO;
        segmentIdx = fileIO.getSegmentId();
    }

    /**
     * @return Absolute WAL segment file index (incremental counter).
     */
    public long getSegmentId(){
        return segmentIdx;
    }
}
