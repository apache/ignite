/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Result of executing a durable background task.
 * <p/>
 * There may be the following states:
 * <ul>
 *   <li>{@link #completed Completed} - the task has completed its execution and should be deleted.</li>
 *   <li>{@link #restart Restart} - the task has not yet completed its execution and must be restarted.</li>
 * </ul>
 */
public class DurableBackgroundTaskResult {
    /** Completed state. */
    private static final Object COMPLETED = new Object();

    /** Restarted state. */
    private static final Object RESTART = new Object();

    /** Execution state. */
    private final Object state;

    /** An error occurred while executing the task. */
    @Nullable private final Throwable err;

    /**
     * Constructor.
     *
     * @param res Execution state.
     * @param err An error occurred while executing the task.
     */
    private DurableBackgroundTaskResult(Object res, @Nullable Throwable err) {
        this.state = res;
        this.err = err;
    }

    /**
     * Creation of a completed task execution result that does not require restarting it.
     *
     * @param err An error occurred while executing the task.
     * @return Result of executing a durable background task.
     */
    public static DurableBackgroundTaskResult complete(@Nullable Throwable err) {
        return new DurableBackgroundTaskResult(COMPLETED, err);
    }

    /**
     * Creation of a task execution result that requires its restart.
     *
     * @param err An error occurred while executing the task.
     * @return Result of executing a durable background task.
     */
    public static DurableBackgroundTaskResult restart(@Nullable Throwable err) {
        return new DurableBackgroundTaskResult(RESTART, err);
    }

    /**
     * Checking the completion of the task.
     *
     * @return {@code True} if completed.
     */
    public boolean completed() {
        return state == COMPLETED;
    }

    /**
     * Checking if the task needs to be restarted.
     *
     * @return {@code True} if the task needs to be restarted.
     */
    public boolean restart() {
        return state == RESTART;
    }

    /**
     * Getting a task execution error.
     *
     * @return An error occurred while executing the task.
     */
    @Nullable public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DurableBackgroundTaskResult.class, this);
    }
}
