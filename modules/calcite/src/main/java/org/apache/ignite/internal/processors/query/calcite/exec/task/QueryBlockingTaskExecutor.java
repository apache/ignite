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

import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;

/**
 * Query task executor based on queue with query blocking.
 */
public class QueryBlockingTaskExecutor extends AbstractQueryTaskExecutor {
    /** */
    private final QueryTasksQueue tasksQueue = new QueryTasksQueue();

    /** */
    private IgniteThreadPoolExecutor executor;

    /** */
    public QueryBlockingTaskExecutor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void execute(UUID qryId, long fragmentId, Runnable qryTask) {
        SecurityContext secCtx = ctx.security().securityContext();

        QueryKey qryKey = new QueryKey(qryId, fragmentId);

        executor.execute(
            new QueryAwareTask(qryKey) {
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
                    finally {
                        tasksQueue.unblockQuery(qryKey);
                    }
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        eHnd = ctx.uncaughtExceptionHandler();

        executor = new IgniteThreadPoolExecutor(
            "calciteQry",
            ctx.igniteInstanceName(),
            ctx.config().getQueryThreadPoolSize(),
            ctx.config().getQueryThreadPoolSize(),
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            tasksQueue.blockingQueue(),
            GridIoPolicy.CALLER_THREAD,
            eHnd
        );

        // Prestart threads to ensure that all threads always use queue to poll tasks (without this call worker can
        // get its first task directly from 'execute' method, bypassing tasks queue).
        executor.prestartAllCoreThreads();

        executor.registerMetrics(ctx.metric().registry(metricName(THREAD_POOLS, THREAD_POOL_NAME)));
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        U.shutdownNow(getClass(), executor, log);
    }
}
