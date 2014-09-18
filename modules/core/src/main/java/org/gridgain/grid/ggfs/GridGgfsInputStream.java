/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import java.io.*;

/**
 * {@code GGFS} input stream to read data from the file system.
 * It provides several additional methods for asynchronous access.
 */
public abstract class GridGgfsInputStream extends InputStream implements GridGgfsReader {
    /**
     * Gets file length during file open.
     *
     * @return File length.
     */
    public abstract long length();

    /**
     * Seek to the specified position.
     *
     * @param pos Position to seek to.
     * @throws IOException In case of IO exception.
     */
    public abstract void seek(long pos) throws IOException;

    /**
     * Get the current position in the input stream.
     *
     * @return The current position in the input stream.
     * @throws IOException In case of IO exception.
     */
    public abstract long position() throws IOException;

    /**
     * Read bytes from the given position in the stream to the given buffer.
     * Continues to read until passed buffer becomes filled.
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @throws IOException In case of IO exception.
     */
    public abstract void readFully(long pos, byte[] buf) throws IOException;

    /**
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @param off Offset in the buffer from which stream data should be written.
     * @param len The number of bytes to read.
     * @throws IOException In case of IO exception.
     */
    public abstract void readFully(long pos, byte[] buf, int off, int len) throws IOException;

    /**
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @param off Offset in the buffer from which stream data should be written.
     * @param len The number of bytes to read.
     * @return Total number of bytes read into the buffer, or -1 if there is no more data (EOF).
     * @throws IOException In case of IO exception.
     */
    @Override public abstract int read(long pos, byte[] buf, int off, int len) throws IOException;
}
