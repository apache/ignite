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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.security.thread.SecurityAwareStripedThreadPoolExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;

/**
 * Query task executor.
 */
public class QueryTaskExecutorImpl extends AbstractService implements QueryTaskExecutor, Thread.UncaughtExceptionHandler {
    /** */
    public static final String THREAD_POOL_NAME = "CalciteQueryExecutor";

    /** */
    private IgniteStripedThreadPoolExecutor stripedThreadPoolExecutor;

    /** */
    private Thread.UncaughtExceptionHandler eHnd;

    /** */
    public QueryTaskExecutorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param stripedThreadPoolExecutor Executor.
     */
    public void stripedThreadPoolExecutor(IgniteStripedThreadPoolExecutor stripedThreadPoolExecutor) {
        this.stripedThreadPoolExecutor = stripedThreadPoolExecutor;
    }

    /**
     * @param eHnd Uncaught exception handler.
     */
    public void exceptionHandler(Thread.UncaughtExceptionHandler eHnd) {
        this.eHnd = eHnd;
    }

    /** {@inheritDoc} */
    @Override public void execute(UUID qryId, long fragmentId, Runnable qryTask) {
        stripedThreadPoolExecutor.execute(
            () -> {
                try {
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
            },
            hash(qryId, fragmentId)
        );
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        exceptionHandler(ctx.uncaughtExceptionHandler());

        IgniteStripedThreadPoolExecutor executor = new SecurityAwareStripedThreadPoolExecutor(
            ctx.security(),
            ctx.applicationContext(),
            ctx.config().getQueryThreadPoolSize(),
            ctx.igniteInstanceName(),
            "calciteQry",
            this,
            false,
            0
        );

        stripedThreadPoolExecutor(executor);

        executor.registerMetrics(ctx.metric().registry(metricName(THREAD_POOLS, THREAD_POOL_NAME)));
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        U.shutdownNow(getClass(), stripedThreadPoolExecutor, log);
    }

    /** {@inheritDoc} */
    @Override public void uncaughtException(Thread t, Throwable e) {
        if (eHnd != null)
            eHnd.uncaughtException(t, e);
    }

    /** */
    private static int hash(UUID qryId, long fragmentId) {
        // inlined Objects.hash(...)
        return U.safeAbs(31 * (31 + (qryId != null ? qryId.hashCode() : 0)) + Long.hashCode(fragmentId));
    }
}
