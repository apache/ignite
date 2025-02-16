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

package org.apache.ignite.internal.processors.query.calcite.exec.task;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Abstract query task executor.
 */
public abstract class AbstractQueryTaskExecutor extends AbstractService implements QueryTaskExecutor, Thread.UncaughtExceptionHandler {
    /** */
    public static final String THREAD_POOL_NAME = "CalciteQueryExecutor";

    /** */
    protected final GridKernalContext ctx;

    /** */
    protected Thread.UncaughtExceptionHandler eHnd;

    /** */
    protected AbstractQueryTaskExecutor(GridKernalContext ctx) {
        super(ctx);
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void uncaughtException(Thread t, Throwable e) {
        if (eHnd != null)
            eHnd.uncaughtException(t, e);
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        eHnd = ctx.uncaughtExceptionHandler();

        super.onStart(ctx);
    }

    /** */
    protected class SecurityAwareTask implements Runnable {
        /** */
        private final SecurityContext secCtx;

        /** */
        private final Runnable qryTask;

        /** */
        public SecurityAwareTask(SecurityContext secCtx, Runnable qryTask) {
            this.secCtx = secCtx;
            this.qryTask = qryTask;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try (AutoCloseable ignored = ctx.security().withContext(secCtx)) {
                qryTask.run();
            }
            catch (Throwable e) {
                U.warn(log, "Uncaught exception", e);

                /*
                 * No exceptions are rethrown here to preserve the current thread from being destroyed,
                 * because other queries may be pinned to the current thread id.
                 * However, unrecoverable errors must be processed by FailureHandler.
                 */
                uncaughtException(Thread.currentThread(), e);
            }
        }
    }
}
