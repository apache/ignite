/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop1impl;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;

/**
 * Hadoop combine task implementation for v1 API.
 */
public class GridHadoopV1CombineTask extends GridHadoopTask {
    /** {@inheritDoc} */
    public GridHadoopV1CombineTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext ctx) throws GridInterruptedException, GridException {

    }
}
