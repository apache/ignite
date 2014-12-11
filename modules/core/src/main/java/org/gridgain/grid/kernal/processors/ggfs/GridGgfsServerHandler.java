/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.ggfs.common.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * GGFS server message handler. Server component that is plugged in to the server implementation
 * to handle incoming messages asynchronously.
 */
public interface GridGgfsServerHandler {
    /**
     * Asynchronously handles incoming message.
     *
     * @param ses Client session.
     * @param msg Message to process.
     * @param in Data input. Stream to read from in case if this is a WRITE_BLOCK message.
     * @return Future that will be completed when response is ready or {@code null} if no
     *      response is required.
     */
    @Nullable public IgniteFuture<GridGgfsMessage> handleAsync(GridGgfsClientSession ses,
        GridGgfsMessage msg, DataInput in);

    /**
     * Handles handles client close events.
     *
     * @param ses Session that was closed.
     */
    public void onClosed(GridGgfsClientSession ses);

    /**
     * Stops handling of incoming requests. No server commands will be handled anymore.
     *
     * @throws IgniteCheckedException If error occurred.
     */
    public void stop() throws IgniteCheckedException;
}
