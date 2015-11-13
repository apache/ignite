package org.apache.ignite.internal.processors.odbc.handlers;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Abstract command handler.
 */
public abstract class GridOdbcCommandHandlerAdapter implements GridOdbcCommandHandler {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Log. */
    protected final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    protected GridOdbcCommandHandlerAdapter(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
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
}
