/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Task finished message. Sent when local task finishes execution.
 */
public class GridHadoopTaskFinishedMessage implements GridHadoopMessage {
    /** Finished task info. */
    private GridHadoopTaskInfo taskInfo;

    /** Task finish state. */
    private GridHadoopTaskState state;

    /** Error. */
    private Throwable err;

    /**
     * @param taskInfo Finished task info.
     * @param state Task finish state.
     * @param err Error (optional).
     */
    public GridHadoopTaskFinishedMessage(GridHadoopTaskInfo taskInfo, GridHadoopTaskState state, Throwable err) {
        this.taskInfo = taskInfo;
        this.state = state;
        this.err = err;
    }

    /**
     * @return Finished task info.
     */
    public GridHadoopTaskInfo taskInfo() {
        return taskInfo;
    }

    /**
     * @return Task finish state.
     */
    public GridHadoopTaskState state() {
        return state;
    }

    /**
     * @return Error cause.
     */
    public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTaskFinishedMessage.class, this);
    }
}
