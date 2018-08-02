/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.segment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.UnzipFileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class SegmentRouter {
    /** */
    private File walWorkDir;

    /** WAL archive directory (including consistent ID as subfolder) */
    private File walArchiveDir;

    /** Holder of actual information of latest manipulation on WAL segments. */
    private SegmentAware segmentAware;

    /** */
    private DataStorageConfiguration dsCfg;

    /** */
    private FileWriteAheadLogManager.FileDecompressor decompressor;

    protected final FileIOFactory ioFactory;

    public SegmentRouter(File walWorkDir, File walArchiveDir,
        SegmentAware segmentAware, DataStorageConfiguration dsCfg,
        FileWriteAheadLogManager.FileDecompressor decompressor,
        FileIOFactory ioFactory) {
        this.walWorkDir = walWorkDir;
        this.walArchiveDir = walArchiveDir;
        this.segmentAware = segmentAware;
        this.dsCfg = dsCfg;
        this.decompressor = decompressor;
        this.ioFactory = ioFactory;
    }

    public FileWriteAheadLogManager.FileDescriptor findSegment(long segmentId) throws FileNotFoundException {
        FileWriteAheadLogManager.FileDescriptor fd;

        if (segmentAware.lastArchivedAbsoluteIndex() >= segmentId) {
            fd = new FileWriteAheadLogManager.FileDescriptor(new File(walArchiveDir,
                FileWriteAheadLogManager.FileDescriptor.fileName(segmentId)));
        }
        else {
            long workIdx = segmentId % dsCfg.getWalSegments();

            fd = new FileWriteAheadLogManager.FileDescriptor(
                new File(walWorkDir, FileWriteAheadLogManager.FileDescriptor.fileName(workIdx)),
                segmentId);
        }

        if (!fd.file().exists()) {
            FileWriteAheadLogManager.FileDescriptor zipFile = new FileWriteAheadLogManager.FileDescriptor(
                new File(walArchiveDir, FileWriteAheadLogManager.FileDescriptor.fileName(fd.idx()) + ".zip"));

            if (!zipFile.file().exists()) {
                throw new FileNotFoundException("Both compressed and raw segment files are missing in archive " +
                    "[segmentIdx=" + fd.idx() + "]");
            }

            fd = zipFile;
        }

        return fd;
    }

}
