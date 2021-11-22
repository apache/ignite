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

package org.apache.ignite.internal.processors.localtask;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.INIT;

/**
 * Class for storing the current state of a durable background task.
 *
 * Task execution state transitions:
 * INIT -> PREPARE -> STARTED -> COMPLETED
 *
 * If the task needs to be restarted, it must have status INIT.
 */
public class DurableBackgroundTaskState<R> {
    /**
     * Enumeration of the current state of the task.
     */
    public enum State {
        /** Initial state. */
        INIT,

        /** Preparation for execution state. */
        PREPARE,

        /** Execution state. */
        STARTED,

        /** Completion state. */
        COMPLETED
    }

    /** Current state atomic updater. */
    private static final AtomicReferenceFieldUpdater<DurableBackgroundTaskState, State> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(DurableBackgroundTaskState.class, State.class, "state");

    /** Durable background task. */
    private final DurableBackgroundTask<R> task;

    /** Outside task future. */
    private final GridFutureAdapter<R> outFut;

    /** Task has been saved to the MetaStorage. */
    private final boolean saved;

    /** Current state of the task. */
    private volatile State state = INIT;

    /** Converted from another task. */
    private final boolean converted;

    /**
     * Constructor.
     *
     * @param task   Durable background task.
     * @param outFut Outside task future.
     * @param saved  Task has been saved to the MetaStorage.
     */
    public DurableBackgroundTaskState(
        DurableBackgroundTask<R> task,
        GridFutureAdapter<R> outFut,
        boolean saved,
        boolean converted
    ) {
        this.task = task;
        this.outFut = outFut;
        this.saved = saved;
        this.converted = converted;
    }

    /**
     * Getting durable background task.
     *
     * @return Durable background task.
     */
    public DurableBackgroundTask<R> task() {
        return task;
    }

    /**
     * Getting outside task future.
     *
     * @return Outside task future.
     */
    public GridFutureAdapter<R> outFuture() {
        return outFut;
    }

    /**
     * Check if the task has been saved to the MetaStorage.
     *
     * @return {@code True} if stored in the MetaStorage.
     */
    public boolean saved() {
        return saved;
    }

    /**
     * Getting current state of the task.
     *
     * @return Current state of the task.
     */
    public State state() {
        return state;
    }

    /**
     * Set the current state of the task.
     *
     * @param s New current state of the task.
     */
    public void state(State s) {
        state = s;
    }

    /**
     * Atomically sets of the current task state.
     *
     * @param exp Expected state.
     * @param newState New state.
     * @return {@code True} if successful.
     */
    public boolean state(State exp, State newState) {
        return STATE_UPDATER.compareAndSet(this, exp, newState);
    }

    /**
     * Check if the task has been converted from another.
     *
     * @return {@code True} if it was converted from another task.
     */
    public boolean converted() {
        return converted;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DurableBackgroundTaskState.class, this);
    }
}
