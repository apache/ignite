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

import java.io.File;
import java.io.FileNotFoundException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;

import static org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor.fileName;

/**
 * Class for manage of segment file location.
 */
public class SegmentRouter {
    /** */
    public static final String ZIP_SUFFIX = ".zip";

    /** */
    private File walWorkDir;

    /** WAL archive directory (including consistent ID as subfolder) */
    private File walArchiveDir;

    /** Holder of actual information of latest manipulation on WAL segments. */
    private SegmentAware segmentAware;

    /** */
    private DataStorageConfiguration dsCfg;

    /**
     * @param walWorkDir WAL work directory.
     * @param walArchiveDir WAL archive directory.
     * @param segmentAware Holder of actual information of latest manipulation on WAL segments.
     * @param dsCfg Data storage configuration.
     */
    public SegmentRouter(
        File walWorkDir,
        File walArchiveDir,
        SegmentAware segmentAware,
        DataStorageConfiguration dsCfg) {
        this.walWorkDir = walWorkDir;
        this.walArchiveDir = walArchiveDir;
        this.segmentAware = segmentAware;
        this.dsCfg = dsCfg;
    }

    /**
     * Find file which represent given segment.
     *
     * @param segmentId Segment for searching.
     * @return Actual file description.
     * @throws FileNotFoundException If file does not exist.
     */
    public FileDescriptor findSegment(long segmentId) throws FileNotFoundException {
        FileDescriptor fd;

        if (segmentAware.lastArchivedAbsoluteIndex() >= segmentId || !isArchiverEnabled())
            fd = new FileDescriptor(new File(walArchiveDir, fileName(segmentId)));
        else
            fd = new FileDescriptor(new File(walWorkDir, fileName(segmentId % dsCfg.getWalSegments())), segmentId);

        if (!fd.file().exists()) {
            FileDescriptor zipFile = new FileDescriptor(new File(walArchiveDir, fileName(fd.idx()) + ZIP_SUFFIX));

            if (!zipFile.file().exists()) {
                throw new FileNotFoundException("Both compressed and raw segment files are missing in archive " +
                    "[segmentIdx=" + fd.idx() + "]");
            }

            fd = zipFile;
        }

        return fd;
    }

    /**
     * @return {@code true} If archive folder exists.
     */
    public boolean hasArchive() {
        return !walWorkDir.getAbsolutePath().equals(walArchiveDir.getAbsolutePath());
    }

    /**
     * @return WAL working directory.
     */
    public File getWalWorkDir() {
        return walWorkDir;
    }

    /**
     * @return WAL archive directory.
     */
    public File getWalArchiveDir() {
        return walArchiveDir;
    }

    /**
     * Returns {@code true} if archiver is enabled.
     */
    private boolean isArchiverEnabled() {
        if (walArchiveDir != null && walWorkDir != null)
            return !walArchiveDir.equals(walWorkDir);

        return !new File(dsCfg.getWalArchivePath()).equals(new File(dsCfg.getWalPath()));
    }
}
