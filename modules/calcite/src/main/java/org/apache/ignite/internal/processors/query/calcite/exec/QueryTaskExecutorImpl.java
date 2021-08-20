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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;

/** */
public class QueryTaskExecutorImpl implements QueryTaskExecutor, Thread.UncaughtExceptionHandler {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(QueryTaskExecutorImpl.class);

    /** */
    private final StripedThreadPoolExecutor stripedThreadPoolExecutor;

    /** */
    private Thread.UncaughtExceptionHandler eHnd;

    /**
     * @param stripedThreadPoolExecutor Executor.
     */
    public QueryTaskExecutorImpl(StripedThreadPoolExecutor stripedThreadPoolExecutor) {
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
                    LOG.warn("Uncaught exception", e);

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
    @Override public CompletableFuture<?> submit(UUID qryId, long fragmentId, Runnable qryTask) {
        return stripedThreadPoolExecutor.submit(qryTask, hash(qryId, fragmentId));
    }

    /** Releases resources. */
    public void tearDown() {
        stripedThreadPoolExecutor.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void uncaughtException(Thread t, Throwable e) {
        if (eHnd != null)
            eHnd.uncaughtException(t, e);
    }

    /** */
    private static int hash(UUID qryId, long fragmentId) {
        // inlined Objects.hash(...)
        return IgniteUtils.safeAbs(31 * (31 + (qryId != null ? qryId.hashCode() : 0)) + Long.hashCode(fragmentId));
    }
}
