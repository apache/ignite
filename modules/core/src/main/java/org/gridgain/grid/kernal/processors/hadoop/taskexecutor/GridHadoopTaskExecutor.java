/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

/**
 * TODO write doc
 */
public class GridHadoopTaskExecutor {
    void run(GridHadoopTaskInfo info, GridHadoopTask task) throws Exception {
        try (GridHadoopTaskOutput out = createOutput(info)) {
            GridHadoopTaskContext ctx = null;

            task.run(ctx);

            out.finish();
        }
    }

    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) {
        return null;
    }
}
