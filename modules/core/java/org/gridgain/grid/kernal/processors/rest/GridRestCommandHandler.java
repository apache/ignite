/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;

/**
 * Command handler.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridRestCommandHandler {
    /**
     * @param cmd Command to check.
     * @return {@code True} if command is supported.
     */
    public boolean supported(GridRestCommand cmd);

    /**
     * @param req Request.
     * @return Future.
     */
    public GridFuture<GridRestResponse> handleAsync(GridRestRequest req);
}
