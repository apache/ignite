package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.odbc.handlers.GridOdbcCommandHandler;
import org.apache.ignite.internal.processors.odbc.handlers.GridOdbcQueryCommandHandler;
import org.apache.ignite.internal.processors.odbc.protocol.GridTcpOdbcServer;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.internal.visor.util.VisorClusterGroupEmptyException;
import org.apache.ignite.lang.IgniteInClosure;
import org.jsr166.LongAdder8;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

/**
 * ODBC processor.
 */
public class GridOdbcProcessor extends GridProcessorAdapter {

    /** OBCD TCP Server. */
    private GridTcpOdbcServer srv;

    /** Workers count. */
    private final LongAdder8 workersCnt = new LongAdder8();

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Start Latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Command handlers. */
    protected final Map<Integer, GridOdbcCommandHandler> handlers = new HashMap<>();

    /** Protocol handler. */
    private final GridOdbcProtocolHandler protoHnd = new GridOdbcProtocolHandler() {
        @Override public GridOdbcResponse handle(GridOdbcRequest req) throws IgniteCheckedException {
            return handleAsync(req).get();
        }

        @Override public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req) {
            return handleAsync0(req);
        }
    };

    /**
     * @param req Request.
     * @return Future.
     */
    private IgniteInternalFuture<GridOdbcResponse> handleAsync0(final GridOdbcRequest req) {
        if (!busyLock.tryReadLock())
            return new GridFinishedFuture<>(
                    new IgniteCheckedException("Failed to handle request (received request while stopping grid)."));

        try {
            final GridWorkerFuture<GridOdbcResponse> fut = new GridWorkerFuture<>();

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), "odbc-proc-worker", log) {
                @Override protected void body() {
                    try {
                        IgniteInternalFuture<GridOdbcResponse> res = handleRequest(req);

                        res.listen(new IgniteInClosure<IgniteInternalFuture<GridOdbcResponse>>() {
                            @Override public void apply(IgniteInternalFuture<GridOdbcResponse> f) {
                                try {
                                    fut.onDone(f.get());
                                }
                                catch (IgniteCheckedException e) {
                                    fut.onDone(e);
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Client request execution failed with error.", e);

                        fut.onDone(U.cast(e));

                        if (e instanceof Error)
                            throw e;
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                ctx.getRestExecutorService().execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on ODBC executor service). " +
                    "Will attempt to process request in the current thread instead.", e);

                w.run();
            }

            return fut;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * @param req Request.
     * @return Future.
     */
    private IgniteInternalFuture<GridOdbcResponse> handleRequest(final GridOdbcRequest req) {
        if (startLatch.getCount() > 0) {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to handle request " +
                        "(protocol handler was interrupted when awaiting grid start).", e));
            }
        }

        if (log.isDebugEnabled())
            log.debug("Received request from client: " + req);

        if (ctx.security().enabled()) {
            // TODO: Implement security checks.
        }

        GridOdbcCommandHandler hnd = handlers.get(req.command());

        IgniteInternalFuture<GridOdbcResponse> res = hnd == null ? null : hnd.handleAsync(req);

        if (res == null)
            return new GridFinishedFuture<>(
                    new IgniteCheckedException("Failed to find registered handler for command: " + req.command()));

        return res.chain(new C1<IgniteInternalFuture<GridOdbcResponse>, GridOdbcResponse>() {
            @Override public GridOdbcResponse apply(IgniteInternalFuture<GridOdbcResponse> f) {
                GridOdbcResponse res;

                boolean failed = false;

                try {
                    res = f.get();
                }
                catch (Exception e) {
                    failed = true;

                    if (!X.hasCause(e, VisorClusterGroupEmptyException.class))
                        LT.error(log, e, "Failed to handle request: " + req.command());

                    if (log.isDebugEnabled())
                        log.debug("Failed to handle request [req=" + req + ", e=" + e + "]");

                    res = new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
                }

                assert res != null;

                if (ctx.security().enabled() && !failed) {
                    // TODO: implement securinty checks.
                }

                return res;
            }
        });
    }

    /**
     * @param ctx Kernal context.
     */
    public GridOdbcProcessor(GridKernalContext ctx) {
        super(ctx);

        srv = new GridTcpOdbcServer(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (isOdbcEnabled()) {

            // Register handlers.
            addHandler(new GridOdbcQueryCommandHandler(ctx));

            srv.start(protoHnd);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (isOdbcEnabled()) {
            srv.stop();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (isOdbcEnabled()) {
            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("ODBC processor started.");
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void onKernalStop(boolean cancel) {
        if (isOdbcEnabled()) {
            busyLock.writeLock();

            boolean interrupted = Thread.interrupted();

            while (workersCnt.sum() != 0) {
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted)
                Thread.currentThread().interrupt();

            // Safety.
            startLatch.countDown();

            if (log.isDebugEnabled())
                log.debug("ODBC processor stopped.");
        }
    }

    /**
     * @return Whether or not ODBC is enabled.
     */
    public boolean isOdbcEnabled() {
        return ctx.config().getOdbcConfiguration().isEnabled();
    }

    /**
     * @param hnd Command handler.
     */
    private void addHandler(GridOdbcCommandHandler hnd) {
        assert !handlers.containsValue(hnd);

        if (log.isDebugEnabled())
            log.debug("Added ODBC command handler: " + hnd);

        for (int cmd : hnd.supportedCommands()) {
            assert !handlers.containsKey(cmd) : cmd;

            handlers.put(cmd, hnd);
        }
    }
}
