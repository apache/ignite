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
package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;

/**
 * Result of archiving (movement) operation
 * Replacement of generic T3-Tuple
 */
class SegmentArchiveResult {
    /** Absolute WAL segment file index. */
    private final long absWalIdx;

    /** Original work file. May and most likely to be used for new WAL round */
    private final File origWorkFile;

    /** Destination archive file. This file is completed and closed archive segment */
    private final File dstArchiveFile;

    /**
     * Creates result
     * @param absWalIdx Absolute wal index.
     * @param origWorkFile Orig work file.
     * @param dstArchiveFile Dst archive file.
     */
    SegmentArchiveResult(long absWalIdx, File origWorkFile, File dstArchiveFile) {
        this.absWalIdx = absWalIdx;
        this.origWorkFile = origWorkFile;
        this.dstArchiveFile = dstArchiveFile;
    }

    /** @return {@link #absWalIdx} */
    long getAbsIdx() {
        return absWalIdx;
    }

    /** @return {@link #origWorkFile} */
    File getOrigWorkFile() {
        return origWorkFile;
    }

    /** @return {@link #dstArchiveFile} */
    File getDstArchiveFile() {
        return dstArchiveFile;
    }
}
