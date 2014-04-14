/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Hadoop job tracker.
 */
public class GridHadoopJobTracker extends GridHadoopComponent {
    /** System cache. */
    private GridCache<Object, Object> sysCache;

    /** Map-reduce execution planner. */
    private GridHadoopMapReducePlanner mrPlanner;

    /** Block resolver. */
    private GridHadoopBlockResolver blockRslvr;

    /** {@inheritDoc} */
    @Override public void onKernalStart() {
        super.onKernalStart();

        sysCache = ctx.kernalContext().cache().cache(ctx.systemCacheName());

        blockRslvr = ctx.blockResolver();

        mrPlanner = ctx.planner();
    }

    /**
     * Submits execution of Hadoop job to grid.
     *
     * @param jobId Job ID.
     * @param info Job info.
     * @return Job completion future.
     */
    public GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo info) {
        try {
            Collection<GridHadoopBlock> blocks = blockRslvr.getInputBlocks(jobId, info);

            initBlockNodes(blocks);

            GridHadoopMapReducePlan mrPlan = mrPlanner.preparePlan(blocks, ctx.nodes(), info, null);

            GridHadoopJobMetadata meta = new GridHadoopJobMetadata(jobId);

            meta.mapReducePlan(mrPlan);

            sysCache.put(jobId, meta);

            return null;
        }
        catch (GridException e) {
            return new GridFinishedFutureEx<>(e);
        }
    }

    /**
     * Gets hadoop job status for given job ID.
     *
     * @param jobId Job ID to get status for.
     * @return Job status for given job ID or {@code null} if job was not found.
     */
    @Nullable public GridHadoopJobStatus status(GridHadoopJobId jobId) {
        return null;
    }

    /**
     * Callback from task executor invoked when a task has been finished.
     *
     * @param taskInfo Task info.
     * @param status Task status.
     */
    public void onTaskFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {

    }

    /**
     * @param blocks Blocks to init nodes for.
     */
    private void initBlockNodes(Collection<GridHadoopBlock> blocks) {

    }
}
