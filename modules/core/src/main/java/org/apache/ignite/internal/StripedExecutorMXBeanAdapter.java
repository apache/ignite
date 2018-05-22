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

package org.apache.ignite.internal;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.mxbean.StripedExecutorMXBean;

/**
 * Adapter for {@link StripedExecutorMXBean} which delegates all method calls to the underlying
 * {@link ExecutorService} instance.
 */
public class StripedExecutorMXBeanAdapter implements StripedExecutorMXBean {
    /** */
    private final StripedExecutor exec;

    /**
     * @param exec Executor service
     */
    StripedExecutorMXBeanAdapter(StripedExecutor exec) {
        assert exec != null;

        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override public void checkStarvation() {
        exec.checkStarvation();
    }

    /** {@inheritDoc} */
    @Override public int getStripesCount() {
        return exec.stripes();
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        return exec.isShutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        return exec.isTerminated();
    }

    /** {@inheritDoc} */
    @Override public int getTotalQueueSize() {
        return exec.queueSize();
    }

    /** {@inheritDoc} */
    @Override public long getTotalCompletedTasksCount() {
        return exec.completedTasks();
    }

    /** {@inheritDoc} */
    @Override public long[] getStripesCompletedTasksCounts() {
        return exec.stripesCompletedTasks();
    }

    /** {@inheritDoc} */
    @Override public int getActiveCount() {
        return exec.activeStripesCount();
    }

    /** {@inheritDoc} */
    @Override public boolean[] getStripesActiveStatuses() {
        return exec.stripesActiveStatuses();
    }

    /** {@inheritDoc} */
    @Override public int[] getStripesQueueSizes() {
        return exec.stripesQueueSizes();
    }
}
