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
import org.gridgain.grid.util.typedef.*;

/**
 * TODO write doc
 */
public class GridHadoopTaskExecutor extends GridHadoopManager {
    GridFuture<?> run(final GridHadoopTaskInfo info, final GridHadoopTask task) throws Exception {
        GridFuture<Void> fut = new GridFutureAdapter<>(ctx.kernalContext());

        ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<GridFuture<?>>() {
            @Override
            public GridFuture<?> call() throws Exception {
                try (GridHadoopTaskOutput out = createOutput(info);
                     GridHadoopTaskInput in = createInput(info)) {
                    GridHadoopTaskContext ctx = null;

                    task.run(ctx);

                    return out.finish();
                }
            }
        }, false).listenAsync(new CIX1<GridFuture<GridFuture<?>>>() {
            @Override public void applyx(GridFuture<GridFuture<?>> f) throws GridException {

            }
        });

        return null;
    }

    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) {
        return null;
    }

    private GridHadoopTaskInput createInput(GridHadoopTaskInfo taskInfo) {
        return null;
    }
}
