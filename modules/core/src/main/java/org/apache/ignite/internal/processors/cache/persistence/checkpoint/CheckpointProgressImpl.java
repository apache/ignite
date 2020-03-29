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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;

/**
 * Data class representing the state of running/scheduled checkpoint.
 */
public class CheckpointProgressImpl implements CheckpointProgress {
    /** Scheduled time of checkpoint. */
    private volatile long nextCpNanos;

    /** Current checkpoint state. */
    private volatile AtomicReference<CheckpointState> state = new AtomicReference(CheckpointState.SCHEDULED);

    /** Future which would be finished when corresponds state is set. */
    private final Map<CheckpointState, GridFutureAdapter> stateFutures = new ConcurrentHashMap<>();

    /** Cause of fail, which has happened during the checkpoint or null if checkpoint was successful. */
    private volatile Throwable failCause;

    /** Flag indicates that snapshot operation will be performed after checkpoint. */
    private volatile boolean nextSnapshot;

    /** Snapshot operation that should be performed if {@link #nextSnapshot} set to true. */
    private volatile SnapshotOperation snapshotOperation;

    /** Wakeup reason. */
    private String reason;

    /** Partitions destroy queue. */
    private final PartitionDestroyQueue destroyQueue = new PartitionDestroyQueue();

    /**
     * @param cpFreq Timeout until next checkpoint.
     */
    public CheckpointProgressImpl(long cpFreq) {
        nextCpNanos = System.nanoTime() + U.millisToNanos(cpFreq);
    }

    /**
     * @return {@code true} If checkpoint already started but have not finished yet.
     */
    @Override public boolean inProgress() {
        return greaterOrEqualTo(LOCK_RELEASED) && !greaterOrEqualTo(FINISHED);
    }

    /**
     * @param expectedState Expected state.
     * @return {@code true} if current state equal to given state.
     */
    public boolean greaterOrEqualTo(CheckpointState expectedState) {
        return state.get().ordinal() >= expectedState.ordinal();
    }

    /**
     * @param state State for which future should be returned.
     * @return Existed or new future which corresponds to the given state.
     */
    @Override public GridFutureAdapter futureFor(CheckpointState state) {
        GridFutureAdapter stateFut = stateFutures.computeIfAbsent(state, (k) -> new GridFutureAdapter());

        if (greaterOrEqualTo(state) && !stateFut.isDone())
            stateFut.onDone(failCause);

        return stateFut;
    }

    /**
     * Mark this checkpoint execution as failed.
     *
     * @param error Causal error of fail.
     */
    @Override public void fail(Throwable error) {
        failCause = error;

        transitTo(FINISHED);
    }

    /**
     * Changing checkpoint state if order of state is correct.
     *
     * @param newState New checkpoint state.
     */
    @Override public void transitTo(@NotNull CheckpointState newState) {
        CheckpointState state = this.state.get();

        if (state.ordinal() < newState.ordinal()) {
            this.state.compareAndSet(state, newState);

            doFinishFuturesWhichLessOrEqualTo(newState);
        }
    }

    /**
     * Finishing futures with correct result in direct state order until lastState(included).
     *
     * @param lastState State until which futures should be done.
     */
    private void doFinishFuturesWhichLessOrEqualTo(@NotNull CheckpointState lastState) {
        for (CheckpointState old : CheckpointState.values()) {
            GridFutureAdapter fut = stateFutures.get(old);

            if (fut != null && !fut.isDone())
                fut.onDone(failCause);

            if (old == lastState)
                return;
        }
    }

    /**
     * @return Wakeup reason.
     **/
    public String reason() {
        return reason;
    }

    /**
     * @param reason Wakeup reason.
     */
    public void reason(String reason) {
        this.reason = reason;
    }

    /**
     * @return {@code true} If snapshot operation will be performed after checkpoint.
     */
    public boolean nextSnapshot() {
        return nextSnapshot;
    }

    /**
     * @param nextSnapshot {@code true} If snapshot operation will be performed after checkpoint.
     */
    public void nextSnapshot(boolean nextSnapshot) {
        this.nextSnapshot = nextSnapshot;
    }

    /**
     * @return Next scheduled checkpoint time.
     **/
    public long nextCpNanos() {
        return nextCpNanos;
    }

    /**
     * Scheduled checkpoint time
     *
     * @param nextCpNanos Next checkpoint time.
     */
    public void nextCpNanos(long nextCpNanos) {
        this.nextCpNanos = nextCpNanos;
    }

    /**
     * Snapshot operation that should be performed if {@link #nextSnapshot} set to true.
     *
     * @return {@link #snapshotOperation}
     **/
    public SnapshotOperation snapshotOperation() {
        return snapshotOperation;
    }

    /**
     * @param snapshotOperation Snapshot operation.
     */
    public void snapshotOperation(SnapshotOperation snapshotOperation) {
        this.snapshotOperation = snapshotOperation;
    }

    /**
     * @return Partitions destroy queue.
     */
    public PartitionDestroyQueue destroyQueue() {
        return destroyQueue;
    }
}
