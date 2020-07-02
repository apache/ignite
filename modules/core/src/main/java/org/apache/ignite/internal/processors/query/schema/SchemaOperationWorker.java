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

package org.apache.ignite.internal.processors.query.schema;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Schema operation executor.
 */
public class SchemaOperationWorker extends GridWorker {
    /** Query processor */
    private final GridQueryProcessor qryProc;

    /** Deployment ID. */
    private final IgniteUuid depId;

    /** Target operation. */
    private final SchemaAbstractOperation op;

    /** No-op flag. */
    private final boolean nop;

    /** Whether cache started. */
    private final boolean cacheRegistered;

    /** Type descriptor. */
    private final QueryTypeDescriptorImpl type;

    /** Operation future. */
    private final GridFutureAdapter fut;

    /** Public operation future. */
    private final GridFutureAdapter pubFut;

    /** Start guard. */
    private final AtomicBoolean startGuard = new AtomicBoolean();

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancelToken = new SchemaIndexOperationCancellationToken();

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param qryProc Query processor.
     * @param depId Deployment ID.
     * @param op Target operation.
     * @param nop No-op flag.
     * @param err Predefined error.
     * @param cacheRegistered Whether cache is registered in indexing at this point.
     * @param type Type descriptor (if available).
     */
    public SchemaOperationWorker(GridKernalContext ctx, GridQueryProcessor qryProc, IgniteUuid depId,
        SchemaAbstractOperation op, boolean nop, @Nullable SchemaOperationException err, boolean cacheRegistered,
        @Nullable QueryTypeDescriptorImpl type) {
        super(ctx.igniteInstanceName(), workerName(op), ctx.log(SchemaOperationWorker.class));

        this.qryProc = qryProc;
        this.depId = depId;
        this.op = op;
        this.nop = nop;
        this.cacheRegistered = cacheRegistered;
        this.type = type;

        fut = new GridFutureAdapter();

        if (err != null)
            fut.onDone(err);
        else if (nop || (!cacheRegistered && !(op instanceof SchemaAddQueryEntityOperation)))
            fut.onDone();

        pubFut = publicFuture(fut);
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            // Execute.
            qryProc.processSchemaOperationLocal(op, type, depId, cancelToken);

            fut.onDone();
        }
        catch (Throwable e) {
            fut.onDone(QueryUtils.wrapIfNeeded(e));
        }
    }

    /**
     * Perform initialization routine.
     *
     * @return This instance.
     */
    public SchemaOperationWorker start() {
        if (startGuard.compareAndSet(false, true)) {
            if (!fut.isDone())
                new IgniteThread(this).start();
        }

        return this;
    }

    /**
     * Chain the future making sure that operation is completed after local schema is updated.
     *
     * @param fut Current future.
     * @return Chained future.
     */
    @SuppressWarnings("unchecked")
    private GridFutureAdapter<?> publicFuture(GridFutureAdapter fut) {
        final GridFutureAdapter<?> chainedFut = new GridFutureAdapter<>();

        fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override public void apply(IgniteInternalFuture fut) {
                Exception err = null;

                try {
                    fut.get();

                    if (cacheRegistered && !nop)
                        qryProc.onLocalOperationFinished(op, type);
                }
                catch (Exception e) {
                    err = e;
                }
                finally {
                    chainedFut.onDone(null, err);
                }
            }
        });

        return chainedFut;
    }

    /**
     * @return No-op flag.
     */
    public boolean nop() {
        return nop;
    }

    /**
     * @return Whether cache is registered.
     */
    public boolean cacheRegistered() {
        return cacheRegistered;
    }

    /**
     * Cancel operation.
     */
    @Override public void cancel() {
        if (cancelToken.cancel())
            super.cancel();
    }

    /**
     * @return Operation.
     */
    public SchemaAbstractOperation operation() {
        return op;
    }

    /**
     * @return Future completed when operation is ready.
     */
    public IgniteInternalFuture future() {
        return pubFut;
    }

    /**
     * @return Worker name.
     */
    private static String workerName(SchemaAbstractOperation op) {
        return "schema-op-worker-" + op.id();
    }
}
