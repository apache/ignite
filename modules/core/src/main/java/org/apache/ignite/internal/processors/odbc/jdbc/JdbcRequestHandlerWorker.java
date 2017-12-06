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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC request handler worker to maintain single threaded transactional execution of SQL statements when MVCC is on.
 */
class JdbcRequestHandlerWorker extends GridWorker {
    /** Requests queue.*/
    private final LinkedBlockingQueue<T2<JdbcRequest, GridFutureAdapter<ClientListenerResponse>>> queue =
        new LinkedBlockingQueue<>();

    /** Start state guard. */
    private final AtomicBoolean startState = new AtomicBoolean();

    /** Finish state guard. */
    private final AtomicBoolean finishState = new AtomicBoolean();

    /** Handler.*/
    private final JdbcRequestHandler hnd;

    /**
     * Constructor.
     * @param igniteInstanceName Instance name.
     * @param log Logger.
     * @param hnd Handler.
     */
    JdbcRequestHandlerWorker(@Nullable String igniteInstanceName, IgniteLogger log,
        JdbcRequestHandler hnd) {
        super(igniteInstanceName, "jdbc-request-handler-worker", log);

        A.notNull(hnd, "handler");

        this.hnd = hnd;
    }

    /**
     * Start this worker.
     */
    void start() {
        if (startState.compareAndSet(false, true))
            new IgniteThread(this).start();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        if (finishState.compareAndSet(false, true))
            super.cancel();
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        while (!finishState.get()) {
            T2<JdbcRequest, GridFutureAdapter<ClientListenerResponse>> req = queue.take();

            GridFutureAdapter<ClientListenerResponse> fut = req.get2();

            ClientListenerResponse res = hnd.doHandle(req.get1());

            fut.onDone(res);
        }
    }

    /**
     * Initiate request processing.
     * @param req Request.
     * @return Future to track request processing.
     */
    GridFutureAdapter<ClientListenerResponse> process(JdbcRequest req) {
        if (!startState.get())
            throw new IllegalStateException();

        // TODO finish

        GridFutureAdapter<ClientListenerResponse> fut = new GridFutureAdapter<>();

        queue.add(new T2<>(req, fut));

        return fut;
    }
}
