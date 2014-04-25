/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Abstract command handler.
 */
public abstract class GridRestCommandHandlerAdapter implements GridRestCommandHandler {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList();

    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Log. */
    protected final GridLogger log;

    /**
     * @param ctx Context.
     */
    protected GridRestCommandHandlerAdapter(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /**
     * Return missing parameter error message.
     *
     * @param param Parameter name.
     * @return Missing parameter error message.
     */
    protected static String missingParameter(String param) {
        return "Failed to find mandatory parameter in request: " + param;
    }

    /**
     * Return invalid numeric parameter error message.
     *
     * @param param Parameter name.
     * @return Invalid numeric parameter error message.
     */
    protected static String invalidNumericParameter(String param) {
        return "Failed to parse numeric parameter: " + param;
    }
}
