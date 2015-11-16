package org.apache.ignite.internal.processors.odbc.handlers;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;

import java.util.Collection;

/**
 * Command handler.
 */
public interface GridOdbcCommandHandler {
    /**
     * @return Collection of supported commands.
     */
    public Collection<Integer> supportedCommands();

    /**
     * @param req Request.
     * @return Future.
     */
    public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req);
}
