/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.GridHadoopTaskCancelledException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.text.*;

/**
 * Extended Hadoop v1 task.
 */
public abstract class GridHadoopV1Task extends GridHadoopTask {
    /** Indicates that this task is to be cancelled. */
    private boolean isCancelled;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     */
    protected GridHadoopV1Task(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /**
     * Gets file name for that task result.
     *
     * @return File name.
     */
    public String fileName() {
        NumberFormat numFormat = NumberFormat.getInstance();

        numFormat.setMinimumIntegerDigits(5);
        numFormat.setGroupingUsed(false);

        return "part-" + numFormat.format(info().taskNumber());
    }

    /**
     *
     * @param jobConf Job configuration.
     * @param taskCtx Task context.
     * @param directWrite Direct write flag.
     * @param fileName File name.
     * @param attempt Attempt of task.
     * @return Collector.
     * @throws IOException In case of IO exception.
     */
    protected GridHadoopOutputCollector getCollector(JobConf jobConf, GridHadoopTaskContext taskCtx, boolean directWrite,
                                                   @Nullable String fileName, TaskAttemptID attempt) throws IOException {
        return new GridHadoopOutputCollector(jobConf, taskCtx, directWrite, fileName, attempt) {
            /** {@inheritDoc} */
            @Override public void collect(Object key, Object val) throws IOException {
                if (isCancelled)
                    throw new GridHadoopTaskCancelledException("Map task cancelled.");

                super.collect(key, val);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        isCancelled = true;
    }

    /** Returns true if task is cancelled. */
    public boolean isCancelled() {
        return isCancelled;
    }
}
