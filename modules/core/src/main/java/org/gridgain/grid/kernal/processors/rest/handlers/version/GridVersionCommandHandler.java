/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.version;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Handler for {@link GridRestCommand#VERSION} command.
 */
public class GridVersionCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(VERSION);

    /**
     * @param ctx Context.
     */
    public GridVersionCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());

        return new GridFinishedFuture<>(ctx, new GridRestResponse(COMPOUND_VER));
    }
}
