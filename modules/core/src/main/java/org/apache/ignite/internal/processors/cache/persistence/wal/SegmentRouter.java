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

        if (segmentAware.lastArchivedAbsoluteIndex() >= segmentId)
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
}
