/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.odbc.handlers.GridOdbcCommandHandler;
import org.apache.ignite.internal.processors.odbc.handlers.GridOdbcQueryCommandHandler;
import org.apache.ignite.internal.processors.odbc.protocol.GridTcpOdbcServer;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.response.GridOdbcResponse;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.visor.util.VisorClusterGroupEmptyException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * ODBC processor.
 */
public class GridOdbcProcessor extends GridProcessorAdapter {

    /** OBCD TCP Server. */
    private GridTcpOdbcServer srv;

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Start Latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Command handlers. */
    protected final Map<Integer, GridOdbcCommandHandler> handlers = new HashMap<>();

    /** Protocol handler. */
    private final GridOdbcProtocolHandler protoHnd = new GridOdbcProtocolHandler() {
        @Override public GridOdbcResponse handle(GridOdbcRequest req) throws IgniteCheckedException {
            return handle0(req);
        }

        @Override public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req) {
            return new GridFinishedFuture<>(
                    new IgniteCheckedException("Failed to handle request (asynchronous handling is not implemented)."));
        }
    };

    /**
     * @param req Request.
     * @return Response.
     */
    private GridOdbcResponse handle0(final GridOdbcRequest req) throws IgniteCheckedException {
        if (!busyLock.tryReadLock())
            throw new IgniteCheckedException("Failed to handle request (received request while stopping grid).");

        GridOdbcResponse rsp = null;

        try {
            rsp = handleRequest(req);
        }
        finally {
            busyLock.readUnlock();
        }

        return rsp;
    }

    /**
     * @param req Request.
     * @return Future.
     */
    private GridOdbcResponse handleRequest(final GridOdbcRequest req) throws IgniteCheckedException {
        if (startLatch.getCount() > 0) {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException("Failed to handle request " +
                        "(protocol handler was interrupted when awaiting grid start).", e);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Received request from client: " + req);

//        if (ctx.security().enabled()) {
            // TODO: Implement security checks.
//        }

        GridOdbcCommandHandler hnd = handlers.get(req.command());

        GridOdbcResponse rsp;

        try {
            rsp = hnd == null ? null : hnd.handle(req);

            if (rsp == null)
                throw new IgniteCheckedException("Failed to find registered handler for command: " + req.command());
        }
        catch (Exception e) {
            if (!X.hasCause(e, VisorClusterGroupEmptyException.class))
                LT.error(log, e, "Failed to handle request: " + req.command());

            if (log.isDebugEnabled())
                log.debug("Failed to handle request [req=" + req + ", e=" + e + "]");

            rsp = new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
        }

        assert rsp != null;

        return rsp;
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
