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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;

/**
 * Query task executor based on striped pool.
 */
public class StripedQueryTaskExecutor extends AbstractQueryTaskExecutor {
    /** */
    private IgniteStripedThreadPoolExecutor stripedThreadPoolExecutor;

    /** */
    public StripedQueryTaskExecutor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param stripedThreadPoolExecutor Executor.
     */
    public void stripedThreadPoolExecutor(IgniteStripedThreadPoolExecutor stripedThreadPoolExecutor) {
        this.stripedThreadPoolExecutor = stripedThreadPoolExecutor;
    }

    /** {@inheritDoc} */
    @Override public void execute(UUID qryId, long fragmentId, Runnable qryTask) {
        SecurityContext secCtx = ctx.security().securityContext();

        stripedThreadPoolExecutor.execute(new SecurityAwareTask(secCtx, qryTask), hash(qryId, fragmentId));
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        super.onStart(ctx);

        IgniteStripedThreadPoolExecutor executor = new IgniteStripedThreadPoolExecutor(
            ctx.config().getQueryThreadPoolSize(),
            ctx.igniteInstanceName(),
            THREAD_PREFIX,
            this,
            false,
            0
        );

        stripedThreadPoolExecutor(executor);

        executor.registerMetrics(ctx.metric().registry(metricName(THREAD_POOLS, THREAD_POOL_NAME)));

        ctx.pools().addExecutorForStarvationDetection("calcite", executor);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        U.shutdownNow(getClass(), stripedThreadPoolExecutor, log);
    }

    /** */
    private static int hash(UUID qryId, long fragmentId) {
        // inlined Objects.hash(...)
        return U.safeAbs(31 * (31 + (qryId != null ? qryId.hashCode() : 0)) + Long.hashCode(fragmentId));
    }
}
