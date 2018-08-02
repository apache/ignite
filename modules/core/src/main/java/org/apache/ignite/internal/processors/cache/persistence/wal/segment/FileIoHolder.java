/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.segment;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class FileIoHolder implements AutoCloseable {

    public FileIO fileIO;

    String absoluteFilePath;

    /** Holder of actual information of latest manipulation on WAL segments. */
    private SegmentAware segmentAware;

    boolean isWorkSegmentLocked;

    final long segmentId;

    /**
     * @param fileIO File io.
     * @param absoluteFilePath Absolute file path.
     * @param segmentAware Segment aware.
     * @param isWorkSegmentLocked Is segment reserved.
     * @param id
     */
    public FileIoHolder(FileIO fileIO, String absoluteFilePath, SegmentAware segmentAware, boolean isWorkSegmentLocked,
        long id) {
        this.fileIO = fileIO;
        this.absoluteFilePath = absoluteFilePath;
        this.segmentAware = segmentAware;
        this.isWorkSegmentLocked = isWorkSegmentLocked;
        segmentId = id;
    }

    public void close() throws IOException {
        if (isWorkSegmentLocked) {
            segmentAware.releaseWorkSegment(segmentId);
        }
    }
}
