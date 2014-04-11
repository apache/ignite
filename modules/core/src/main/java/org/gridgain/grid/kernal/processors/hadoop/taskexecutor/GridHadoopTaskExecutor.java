/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;

/**
 * TODO write doc
 */
public class GridHadoopTaskExecutor extends GridHadoopManager {
    /**
     * Runs task.
     *
     * @param info Task info.
     * @param task Task.
     * @return Completion future.
     */
    public GridFuture<?> run(final GridHadoopTaskInfo info, final GridHadoopTask task) {
        GridChainedFuture<?> res = new GridChainedFuture<>(ctx.kernalContext());

        ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<GridFuture<?>>() {
            @Override public GridFuture<?> call() throws Exception {
                try (GridHadoopTaskOutput out = createOutput(info);
                     GridHadoopTaskInput in = createInput(info)) {
                    GridHadoopTaskContext ctx = null;

                    task.run(ctx);

                    return out.finish();
                }
            }
        }, false).listenAsync(res);

        return res;
    }

    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) {
        return null;
    }

    private GridHadoopTaskInput createInput(GridHadoopTaskInfo taskInfo) {
        return null;
    }
}
