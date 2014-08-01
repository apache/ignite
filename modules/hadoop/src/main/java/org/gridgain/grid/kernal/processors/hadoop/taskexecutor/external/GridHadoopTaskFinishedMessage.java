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

import java.io.*;

/**
 * Task finished message. Sent when local task finishes execution.
 */
public class GridHadoopTaskFinishedMessage implements GridHadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Finished task info. */
    private GridHadoopTaskInfo taskInfo;

    /** Task finish status. */
    private GridHadoopTaskStatus status;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public GridHadoopTaskFinishedMessage() {
        // No-op.
    }

    /**
     * @param taskInfo Finished task info.
     * @param status Task finish status.
     */
    public GridHadoopTaskFinishedMessage(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
        assert taskInfo != null;
        assert status != null;

        this.taskInfo = taskInfo;
        this.status = status;
    }

    /**
     * @return Finished task info.
     */
    public GridHadoopTaskInfo taskInfo() {
        return taskInfo;
    }

    /**
     * @return Task finish status.
     */
    public GridHadoopTaskStatus status() {
        return status;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTaskFinishedMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        taskInfo.writeExternal(out);
        status.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        taskInfo = new GridHadoopTaskInfo();
        taskInfo.readExternal(in);

        status = new GridHadoopTaskStatus();
        status.readExternal(in);
    }
}
