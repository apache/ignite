/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;

import java.io.*;
import java.text.*;

/**
 * Hadoop task.
 */
public abstract class GridHadoopTask implements Externalizable {
    /** Number format. */
    private static NumberFormat numFormat = NumberFormat.getInstance();

    /** */
    private GridHadoopTaskInfo taskInfo;

    /**
     * Creates task.
     *
     * @param taskInfo Task info.
     */
    public GridHadoopTask(GridHadoopTaskInfo taskInfo) {
        assert taskInfo != null;

        this.taskInfo = taskInfo;
    }

    /**
     * For {@link Externalizable}.
     */
    public GridHadoopTask() {
        // No-op.
    }

    /**
     * Gets task info.
     *
     * @return Task info.
     */
    public GridHadoopTaskInfo info() {
        return taskInfo;
    }

    /**
     * Gets file name for that task result.
     *
     * @return File name.
     */
    public String fileName() {
        numFormat.setMinimumIntegerDigits(5);
        numFormat.setGroupingUsed(false);

        return "part-" + numFormat.format(taskInfo.taskNumber());
    }

    /**
     * Runs task.
     *
     * @param taskCtx Context.
     * @throws GridInterruptedException If interrupted.
     * @throws GridException If failed.
     */
    public abstract void run(GridHadoopTaskContext taskCtx) throws GridException;

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        taskInfo.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert taskInfo == null;

        taskInfo = new GridHadoopTaskInfo();

        taskInfo.readExternal(in);
    }
}
