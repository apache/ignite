// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridIpcSharedMemoryInputStream extends InputStream {
    /** */
    private final GridIpcSharedMemorySpace in;

    /** Stream instance is not thread-safe so we can cache buffer. */
    private byte[] buf = new byte[1];

    /**
     * @param in Space.
     */
    public GridIpcSharedMemoryInputStream(GridIpcSharedMemorySpace in) {
        assert in != null;

        this.in = in;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        try {
            int read = in.read(buf, 0, 1, 0);

            if (read < 0)
                return read;

            return buf[0] & 0xFF;
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len, 0);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int available() throws IOException {
        try {
            return in.unreadCount();
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        in.close();
    }

    /**
     * Forcibly closes spaces and frees all system resources.
     * <p>
     * This method should be called with caution as it may result to the other-party
     * process crash. It is intended to call when there was an IO error during handshake
     * and other party has not yet attached to the space.
     */
    public void forceClose() {
        in.forceClose();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIpcSharedMemoryInputStream.class, this);
    }
}
