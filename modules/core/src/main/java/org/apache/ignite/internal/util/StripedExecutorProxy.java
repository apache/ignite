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

package org.apache.ignite.internal.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapped striped executor which delegates all jobs to target executor service.
 * It is used instead of the striped executor when that one is disabled (striped pool size <= 0).
 */
public class StripedExecutorProxy extends StripedExecutor {
    /** Target executor service */
    private volatile ExecutorService executor;

    /**
     * @param executor Target executor service.
     * @param log Logger.
     */
    public StripedExecutorProxy(ExecutorService executor, final IgniteLogger log) {
        super(1, "", "", log);

        this.executor = executor;
    }

    /** {@inheritDoc} */
    @Override public void checkStarvation() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int stripes() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void execute(int idx, Runnable cmd) {
        execute(cmd);
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        checkExecutor();

        executor.execute(cmd);
    }

    /**
     * Check target executor is not null.
     *
     * @throws IgniteException If executor is null.
     */
    private void checkExecutor() {
        if (executor == null)
            throw new IgniteException("Target executor is not found.");
    }

    /** {@inheritDoc} */
    @NotNull @Override public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        checkExecutor();

        return executor.isShutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        checkExecutor();

        return executor.isTerminated();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int queueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long completedTasks() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long[] stripesCompletedTasks() {
        return new long[] { 0L };
    }

    /** {@inheritDoc} */
    @Override public boolean[] stripesActiveStatuses() {
        return new boolean[] { false };
    }

    /** {@inheritDoc} */
    @Override public int activeStripesCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int[] stripesQueueSizes() {
        return new int[] { 0 };
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StripedExecutorProxy.class, this);
    }
}
