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
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;

/**
 * Simple implementation of {@link FileInputFactory}.
 */
public class SimpleFileInputFactory implements FileInputFactory {

    /** {@inheritDoc} */
    @Override public FileInput createFileInput(long segmentId, FileIO fileIO,
        ByteBufferExpander buf) throws IOException {
        return new SimpleFileInput(fileIO, buf);
    }
}
