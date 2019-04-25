/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.taskexecutor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Task status.
 */
public class HadoopTaskStatus implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private HadoopTaskState state;

    /** */
    private Throwable failCause;

    /** */
    private HadoopCounters cntrs;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public HadoopTaskStatus() {
        // No-op.
    }

    /**
     * Creates new instance.
     *
     * @param state Task state.
     * @param failCause Failure cause (if any).
     */
    public HadoopTaskStatus(HadoopTaskState state, @Nullable Throwable failCause) {
        this(state, failCause, null);
    }

    /**
     * Creates new instance.
     *
     * @param state Task state.
     * @param failCause Failure cause (if any).
     * @param cntrs Task counters.
     */
    public HadoopTaskStatus(HadoopTaskState state, @Nullable Throwable failCause,
        @Nullable HadoopCounters cntrs) {
        assert state != null;

        this.state = state;
        this.failCause = failCause;
        this.cntrs = cntrs;
    }

    /**
     * @return State.
     */
    public HadoopTaskState state() {
        return state;
    }

    /**
     * @return Fail cause.
     */
    @Nullable public Throwable failCause() {
        return failCause;
    }

    /**
     * @return Counters.
     */
    @Nullable public HadoopCounters counters() {
        return cntrs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopTaskStatus.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(state);
        out.writeObject(failCause);
        out.writeObject(cntrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        state = (HadoopTaskState)in.readObject();
        failCause = (Throwable)in.readObject();
        cntrs = (HadoopCounters)in.readObject();
    }
}