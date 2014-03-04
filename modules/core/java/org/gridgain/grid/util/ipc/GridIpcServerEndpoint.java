/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * IPC server endpoint that is capable for client connections accepting.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridIpcServerEndpoint extends Closeable {
    /**
     * Accepts client IPC connection. After client connection is accepted, it can be used
     * for IPC. This method will block until client connects to IPC server endpoint.
     *
     * @return Accepted client connection.
     * @throws GridException If accept failed and the endpoint is not usable anymore.
     */
    public GridIpcEndpoint accept() throws GridException;

    /**
     * Starts configured endpoint implementation.
     *
     * @throws GridException If failed to start server endpoint.
     */
    public void start() throws GridException;

    /**
     * Closes server IPC. After IPC is closed, no further operations can be performed on this
     * object.
     */
    @Override public void close();

    /**
     * Gets port endpoint is bound to.
     * Endpoints who does not bind to any port should return -1.
     *
     * @return Port number.
     */
    public int getPort();

    /**
     * Gets host endpoint is bound to.
     * Endpoints who does not bind to any port should return {@code null}.
     *
     * @return Host.
     */
    @Nullable public String getHost();

    /**
     * Indicates if this endpoint is a management endpoint.
     *
     * @return {@code true} if it's a management endpoint.
     */
    public boolean isManagement();
}
