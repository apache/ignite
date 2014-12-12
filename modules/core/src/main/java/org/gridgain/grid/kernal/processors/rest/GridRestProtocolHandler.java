/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.rest.request.*;

/**
 * Command protocol handler.
 */
public interface GridRestProtocolHandler {
    /**
     * @param req Request.
     * @return Response.
     * @throws IgniteCheckedException In case of error.
     */
    public GridRestResponse handle(GridRestRequest req) throws IgniteCheckedException;

    /**
     * @param req Request.
     * @return Future.
     */
    public IgniteFuture<GridRestResponse> handleAsync(GridRestRequest req);
}
