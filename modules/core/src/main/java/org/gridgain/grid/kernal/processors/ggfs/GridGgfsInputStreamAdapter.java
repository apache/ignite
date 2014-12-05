/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.fs.*;

import java.io.*;

/**
 * Implementation adapter providing necessary methods.
 */
public abstract class GridGgfsInputStreamAdapter extends IgniteFsInputStream {
    /** {@inheritDoc} */
    @Override public long length() {
        return fileInfo().length();
    }

    /**
     * Gets file info for opened file.
     *
     * @return File info.
     */
    public abstract GridGgfsFileInfo fileInfo();

    /**
     * Reads bytes from given position.
     *
     * @param pos Position to read from.
     * @param len Number of bytes to read.
     * @return Array of chunks with respect to chunk file representation.
     * @throws IOException If read failed.
     */
    public abstract byte[][] readChunks(long pos, int len) throws IOException;
}
