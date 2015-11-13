package org.apache.ignite.internal.processors.odbc.handlers;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;
import org.apache.ignite.internal.processors.odbc.protocol.GridOdbcCommand;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.odbc.protocol.GridOdbcCommand.EXECUTE_SQL_QUERY;

/**
 * SQL query handler.
 */
public class GridOdbcQueryCommandHandler extends GridOdbcCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridOdbcCommand> SUPPORTED_COMMANDS = U.sealList(EXECUTE_SQL_QUERY);

    /** Query ID sequence. */
    private static final AtomicLong qryIdGen = new AtomicLong();

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     */
    public GridOdbcQueryCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridOdbcCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());

        switch (req.command()) {
            case EXECUTE_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                        new ExecuteQueryCallable(ctx, req, qryCurs), false);
            }
        }

        return new GridFinishedFuture<>();
    }

    /**
     * Execute query callable.
     */
    private static class ExecuteQueryCallable implements Callable<GridOdbcResponse> {
        /** Kernal context. */
        private GridKernalContext ctx;

        /** Execute query request. */
        private GridOdbcRequest req;

        /** Queries cursors. */
        private ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /**
         * @param ctx Kernal context.
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public ExecuteQueryCallable(GridKernalContext ctx, GridOdbcRequest req,
                                    ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.ctx = ctx;
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridOdbcResponse call() throws Exception {
            long qryId = qryIdGen.getAndIncrement();

            try {
                SqlFieldsQuery qry = new SqlFieldsQuery(req.sqlQuery());

                qry.setArgs(req.arguments());

                IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

                if (cache == null)
                    return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                            "Failed to find cache with name: " + req.cacheName());

                QueryCursor qryCur = cache.query(qry);

                Iterator cur = qryCur.iterator();

                qryCurs.put(qryId, new IgniteBiTuple<>(qryCur, cur));

                GridOdbcQueryResult res = new GridOdbcQueryResult(qryId);

                return new GridOdbcResponse(res);
            }
            catch (Exception e) {
                qryCurs.remove(qryId);

                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }
}
