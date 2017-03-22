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

package org.apache.ignite.internal.processors.query.index;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CountDownLatch;

/**
 * Index change handler.
 */
public class IndexOperationHandler {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Query processor */
    private final GridQueryProcessor qryProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Target operation. */
    private final IndexAbstractOperation op;

    /** Operation future. */
    private final GridFutureAdapter opFut;

    /** Mutex for concurrent access. */
    private final Object mux = new Object();

    /** Cancellation token. */
    private final IndexOperationCancellationToken cancelToken = new IndexOperationCancellationToken();

    /** Init flag. */
    private boolean init;

    /** Worker. */
    private IndexWorker worker;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param qryProc Query processor.
     * @param op Target operation.
     * @param completed Whether this is dummy request which should be considered completed right-away. This is the
     *     case for client nodes and for server node in-progress operations received through discovery data.
     * @param err Error for future to be completed with.
     */
    public IndexOperationHandler(GridKernalContext ctx, GridQueryProcessor qryProc, IndexAbstractOperation op,
        boolean completed, Exception err) {
        this.ctx = ctx;
        this.qryProc = qryProc;
        this.op = op;

        log = ctx.log(IndexOperationHandler.class);

        opFut = new GridFutureAdapter();

        if (completed) {
            init = true;

            if (err != null)
                opFut.onDone(err);
            else
                opFut.onDone();
        }
    }

    /**
     * Perform initialization routine.
     */
    public void init() {
        synchronized (mux) {
            if (!init) {
                init = true;

                if (!cancelToken.isCancelled()) {
                    worker = new IndexWorker(ctx.igniteInstanceName(), workerName(), log);

                    new IgniteThread(worker).start();

                    worker.awaitStart();
                }
            }
        }
    }

    /**
     * Cancel operation.
     */
    public void cancel() {
        synchronized (mux) {
            if (!cancelToken.cancel()) {
                if (worker != null)
                    worker.cancel();
            }
        }
    }

    /**
     * @return Operation.
     */
    public IndexAbstractOperation operation() {
        return op;
    }

    /**
     * @return Future completed when operation is ready.
     */
    public IgniteInternalFuture future() {
        return opFut;
    }

    /**
     * @return Worker name.
     */
    private String workerName() {
        return "index-op-worker" + op.space() + "-" + op.indexName();
    }

    /**
     * Single-shot index worker responsible for operation execution.
     */
    private class IndexWorker extends GridWorker {
        /** Worker start latch. */
        private final CountDownLatch startLatch = new CountDownLatch(1);

        /**
         * Constructor.
         *
         * @param igniteInstanceName Ignite instance name.
         * @param name Worker name.
         * @param log Logger.
         */
        public IndexWorker(@Nullable String igniteInstanceName, String name, IgniteLogger log) {
            super(igniteInstanceName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            startLatch.countDown();

            try {
                qryProc.processIndexOperation(op, cancelToken);

                opFut.onDone();
            }
            catch (Exception e) {
                opFut.onDone(e);
            }
        }

        /**
         * Await start.
         */
        private void awaitStart() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedException("Interrupted while waiting index operation worker start: " +
                    name(), e);
            }
        }
    }
}
