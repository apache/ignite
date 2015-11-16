package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;

/**
 * ODBC command protocol handler.
 */
public interface GridOdbcProtocolHandler {
    /**
     * @param req Request.
     * @return Response.
     * @throws IgniteCheckedException In case of error.
     */
    public GridOdbcResponse handle(GridOdbcRequest req) throws IgniteCheckedException;

    /**
     * @param req Request.
     * @return Future.
     */
    public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req);
}
