/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.gridgain.grid.hadoop.*;

import java.text.*;

/**
 * Extended Hadoop v1 task.
 */
public abstract class GridHadoopV1Task extends GridHadoopTask {
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
}
