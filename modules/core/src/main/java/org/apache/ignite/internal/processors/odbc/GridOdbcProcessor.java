package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.odbc.protocol.GridTcpOdbcServer;

/**
 * ODBC processor.
 */
public class GridOdbcProcessor extends GridProcessorAdapter {

    /** OBCD TCP Server. */
    private GridTcpOdbcServer server;

    /**
     * @param ctx Kernal context.
     */
    protected GridOdbcProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        OdbcConfiguration config = ctx.config().getOdbcConfiguration();

        if (config.isEnabled()) {
            server = new GridTcpOdbcServer(ctx);

            server.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (server != null)
            server.stop();
    }
}
