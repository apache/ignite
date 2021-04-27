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

package org.apache.ignite.internal.processors.localtask;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Simple {@link DurableBackgroundTask} implementation for tests.
 */
class SimpleTask implements DurableBackgroundTask {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Task name. */
    private String name;

    /** Future that will be completed at the beginning of the {@link #executeAsync}. */
    final transient GridFutureAdapter<Void> onExecFut = new GridFutureAdapter<>();

    /** Future that will be returned from the {@link #executeAsync}. */
    final transient GridFutureAdapter<DurableBackgroundTaskResult> taskFut = new GridFutureAdapter<>();

    /**
     * Constructor.
     *
     * @param name Task name.
     */
    public SimpleTask(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<DurableBackgroundTaskResult> executeAsync(GridKernalContext ctx) {
        onExecFut.onDone();

        return taskFut;
    }
}
