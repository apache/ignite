package org.apache.ignite.internal.processors.odbc.handlers;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;
import org.apache.ignite.internal.processors.odbc.protocol.GridOdbcCommand;

import java.util.Collection;

/**
 * Command handler.
 */
public interface GridOdbcCommandHandler {
    /**
     * @return Collection of supported commands.
     */
    public Collection<GridOdbcCommand> supportedCommands();

    /**
     * @param req Request.
     * @return Future.
     */
    public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req);
}
