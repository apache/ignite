/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 * TODO write doc
 */
public class GridHadoopTaskExecutor extends GridHadoopComponent {
    /**
     * Runs tasks.
     *
     * @param tasks Tasks.
     * @return Completion future.
     */
    public void run(Collection<GridHadoopTask> tasks) {
        for (final GridHadoopTask task : tasks)
            ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<GridFuture<?>>() {
                @Override public GridFuture<?> call() throws Exception {
                    GridHadoopTaskInfo info = task.info();

                    try (GridHadoopTaskOutput out = createOutput(info);
                         GridHadoopTaskInput in = createInput(info)) {
                        GridHadoopTaskContext ctx = null;

                        task.run(ctx);

                        return out.finish();
                    }
                }
            }, false);
    }

    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) {
        return null;
    }

    private GridHadoopTaskInput createInput(GridHadoopTaskInfo taskInfo) {
        return null;
    }
}
