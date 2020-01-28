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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 * TODO use {@link org.apache.ignite.internal.util.StripedExecutor}, registered in core pols.
 */
public class QueryTaskExecutorImpl extends AbstractService implements QueryTaskExecutor {
    /** */
    private IgniteStripedThreadPoolExecutor srvc;

    /** */
    public QueryTaskExecutorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void execute(UUID queryId, long fragmentId, Runnable queryTask) {
        srvc.execute(queryTask, hash(queryId, fragmentId));
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        srvc = new IgniteStripedThreadPoolExecutor(
            ctx.config().getQueryThreadPoolSize(),
            ctx.igniteInstanceName(),
            "calciteQry",
            ctx.uncaughtExceptionHandler(),
            true,
            DFLT_THREAD_KEEP_ALIVE_TIME
        );
    }

    /** {@inheritDoc} */
    @Override public void onStop() {
        U.shutdownNow(getClass(), srvc, log);
        srvc = null;
    }

    /** */
    private static int hash(UUID queryId, long fragmentId) {
        // inlined Objects.hash(...)
        return U.safeAbs(31 * (31 + (queryId != null ? queryId.hashCode() : 0)) + Long.hashCode(fragmentId));
    }
}
