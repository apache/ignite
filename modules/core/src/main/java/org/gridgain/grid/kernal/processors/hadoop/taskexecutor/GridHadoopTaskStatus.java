/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.jetbrains.annotations.*;

/**
 * Task status.
 */
public class GridHadoopTaskStatus {
    /** */
    private final GridHadoopTaskState state;

    /** */
    private final Throwable failCause;

    public GridHadoopTaskStatus(GridHadoopTaskState state, @Nullable Throwable failCause) {
        assert state != null;

        this.state = state;
        this.failCause = failCause;
    }

    /**
     * @return State.
     */
    public GridHadoopTaskState state() {
        return state;
    }

    /**
     * @return Fail cause.
     */
    @Nullable public Throwable failCause() {
        return failCause;
    }
}
