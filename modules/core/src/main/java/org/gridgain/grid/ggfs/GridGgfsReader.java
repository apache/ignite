package org.gridgain.grid.ggfs;

import org.gridgain.grid.GridException;

import java.io.*;

public interface GridGgfsReader extends Closeable {
    /**
     *
     * @param pos Position in the input stream to seek.
     * @param buf Buffer into which data is read.
     * @param off Offset in the buffer from which stream data should be written.
     * @param len The number of bytes to read.
     * @return Total number of bytes read into the buffer, or -1 if there is no more data (EOF).
     * @throws GridException In case of any exception.
     */
    int read(long pos, byte[] buf, int off, int len) throws GridException;
}
