/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;

/**
 * Hadoop  task implementation for v2 API.
 */
public class GridHadoopV2TaskImpl implements GridHadoopTask {
    /** task info. */
    private GridHadoopTaskInfo taskInfo;

    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2TaskImpl(GridHadoopTaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskInfo info() {
        return taskInfo;
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext ctx) throws GridInterruptedException, GridException {
    }
}
