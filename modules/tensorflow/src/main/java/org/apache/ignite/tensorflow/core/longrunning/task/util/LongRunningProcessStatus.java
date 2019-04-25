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

package org.apache.ignite.tensorflow.core.longrunning.task.util;

import java.io.Serializable;

/**
 * Long running process status that includes state and exception if exists.
 */
public class LongRunningProcessStatus implements Serializable {
    /** */
    private static final long serialVersionUID = -2958316078722079954L;

    /** Process state. */
    private final LongRunningProcessState state;

    /** Process exception. */
    private final Exception e;

    /**
     * Constructs a new instance of long running process status.
     *
     * @param state Process state.
     */
    public LongRunningProcessStatus(LongRunningProcessState state) {
        this(state, null);
    }

    /**
     * Constructs a new instance of long running process status.
     *
     * @param state Process state.
     * @param e Process exception.
     */
    public LongRunningProcessStatus(LongRunningProcessState state, Exception e) {
        assert state != null : "Process state should not be null";

        this.state = state;
        this.e = e;
    }

    /** */
    public LongRunningProcessState getState() {
        return state;
    }

    /** */
    public Exception getException() {
        return e;
    }
}
