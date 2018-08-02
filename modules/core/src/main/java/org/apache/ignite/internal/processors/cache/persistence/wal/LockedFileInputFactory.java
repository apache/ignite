/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.segment.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.segment.SegmentRouter;

/**
 *
 */
public class LockedFileInputFactory implements FileInputFactory {

    SegmentAware segmentAware;

    SegmentRouter segmentRouter;

    FileIOFactory fileIOFactory;

    public LockedFileInputFactory(
        SegmentAware segmentAware,
        SegmentRouter segmentRouter,
        FileIOFactory fileIOFactory) {
        this.segmentAware = segmentAware;
        this.segmentRouter = segmentRouter;
        this.fileIOFactory = fileIOFactory;
    }

    @Override public FileInput createFileInput(long segmentId, FileIO fileIO, ByteBufferExpander buf) throws IOException {
        return new LockedReadFileInput(
            buf,
            fileIO,
            segmentId,
            segmentAware,
            id -> {
                FileWriteAheadLogManager.FileDescriptor segment = segmentRouter.findSegment(id);

                return segment.toIO(fileIOFactory);
            }
        );
    }
}
