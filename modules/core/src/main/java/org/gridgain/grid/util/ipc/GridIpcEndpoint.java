/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc;

import org.apache.ignite.*;

import java.io.*;

/**
 * GGFS IPC endpoint used for point-to-point communication.
 */
public interface GridIpcEndpoint extends Closeable {
    /**
     * Gets input stream associated with this IPC endpoint.
     *
     * @return IPC input stream.
     * @throws IgniteCheckedException If error occurred.
     */
    public InputStream inputStream() throws IgniteCheckedException;

    /**
     * Gets output stream associated with this IPC endpoint.
     *
     * @return IPC output stream.
     * @throws IgniteCheckedException If error occurred.
     */
    public OutputStream outputStream() throws IgniteCheckedException;

    /**
     * Closes endpoint. Note that IPC endpoint may acquire native resources so it must be always closed
     * once it is not needed.
     */
    @Override public void close();
}
