/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.request.*;

import java.util.*;

/**
 * Command handler.
 */
public interface GridRestCommandHandler {
    /**
     * @return Collection of supported commands.
     */
    public Collection<GridRestCommand> supportedCommands();

    /**
     * @param req Request.
     * @return Future.
     */
    public IgniteFuture<GridRestResponse> handleAsync(GridRestRequest req);
}
