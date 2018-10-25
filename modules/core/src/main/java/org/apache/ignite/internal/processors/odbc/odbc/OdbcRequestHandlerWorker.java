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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * ODBC request handler worker to maintain single threaded transactional execution of SQL statements when MVCC is on.<p>
 * This worker is intended for internal use as a temporary solution and from within {@link OdbcRequestHandler},
 * therefore it does not do any fine-grained lifecycle handling as it relies on existing guarantees from
 * {@link ClientListenerNioListener}.
 */
class OdbcRequestHandlerWorker extends GridWorker {
    /** Requests queue.*/
    private final LinkedBlockingQueue<T2<OdbcRequest, GridFutureAdapter<ClientListenerResponse>>> queue =
        new LinkedBlockingQueue<>();

    /** Handler.*/
    private final OdbcRequestHandler hnd;

    /** Context.*/
    private final GridKernalContext ctx;

    /** Response */
    private static final ClientListenerResponse ERR_RESPONSE = new OdbcResponse(IgniteQueryErrorCode.UNKNOWN,
        "Connection closed.");

    /**
     * Constructor.
     * @param igniteInstanceName Instance name.
     * @param log Logger.
     * @param hnd Handler.
     * @param ctx Kernal context.
     */
    OdbcRequestHandlerWorker(@Nullable String igniteInstanceName, IgniteLogger log, OdbcRequestHandler hnd,
        GridKernalContext ctx) {
        super(igniteInstanceName, "odbc-request-handler-worker", log);

        A.notNull(hnd, "hnd");

        this.hnd = hnd;

        this.ctx = ctx;
    }

    /**
     * Start this worker.
     */
    void start() {
        new IgniteThread(this).start();
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            while (!isCancelled()) {
                T2<OdbcRequest, GridFutureAdapter<ClientListenerResponse>> req = queue.take();

                GridFutureAdapter<ClientListenerResponse> fut = req.get2();

                try {
                    ClientListenerResponse res = hnd.doHandle(req.get1());

                    fut.onDone(res);
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
            }
        }
        finally {
            // Notify indexing that this worker is being stopped.
            try {
                ctx.query().getIndexing().onClientDisconnect();
            }
            catch (Exception e) {
                // No-op.
            }

            // Drain the queue on stop.
            T2<OdbcRequest, GridFutureAdapter<ClientListenerResponse>> req = queue.poll();

            while (req != null) {
                req.get2().onDone(ERR_RESPONSE);

                req = queue.poll();
            }
        }
    }

    /**
     * Initiate request processing.
     * @param req Request.
     * @return Future to track request processing.
     */
    GridFutureAdapter<ClientListenerResponse> process(OdbcRequest req) {
        GridFutureAdapter<ClientListenerResponse> fut = new GridFutureAdapter<>();

        queue.add(new T2<>(req, fut));

        return fut;
    }
}
