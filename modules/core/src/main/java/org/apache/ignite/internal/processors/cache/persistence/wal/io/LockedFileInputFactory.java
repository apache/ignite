/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.io;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;

/**
 * Implementation of factory to provide I/O interfaces for read primitives with files.
 *
 * Creating {@link FileInput} with ability locked segment during reading.
 */
public class LockedFileInputFactory implements FileInputFactory {
    /** Holder of actual information of latest manipulation on WAL segments. */
    private final SegmentAware segmentAware;
    /** Manager of segment location. */
    private final SegmentRouter segmentRouter;
    /** {@link FileIO} factory definition.*/
    private final FileIOFactory fileIOFactory;

    /**
     * @param segmentAware Holder of actual information of latest manipulation on WAL segments.
     * @param segmentRouter Manager of segment location.
     * @param fileIOFactory {@link FileIO} factory definition.
     */
    public LockedFileInputFactory(
        SegmentAware segmentAware,
        SegmentRouter segmentRouter,
        FileIOFactory fileIOFactory) {
        this.segmentAware = segmentAware;
        this.segmentRouter = segmentRouter;
        this.fileIOFactory = fileIOFactory;
    }

    /** {@inheritDoc} */
    @Override public FileInput createFileInput(long segmentId, FileIO fileIO, ByteBufferExpander buf) throws IOException {
        return new LockedReadFileInput(
            buf,
            fileIO,
            segmentId,
            segmentAware,
            id -> {
                FileDescriptor segment = segmentRouter.findSegment(id);

                return segment.toIO(fileIOFactory);
            }
        );
    }
}
