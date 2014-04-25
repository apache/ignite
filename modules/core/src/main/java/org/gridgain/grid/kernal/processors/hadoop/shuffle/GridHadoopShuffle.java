/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.offheap.unsafe.*;

import java.util.concurrent.*;

/**
 * Shuffle.
 */
public class GridHadoopShuffle extends GridHadoopComponent {
    /** */
    private ConcurrentMap<GridHadoopJobId, GridHadoopShuffleJob> jobs = new ConcurrentHashMap<>();

    /** */
    private GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
//        ctx.kernalContext().io().addMessageListener();
    }

    /**
     * @param jobId Task info.
     * @return Shuffle job.
     */
    private GridHadoopShuffleJob job(GridHadoopJobId jobId) throws GridException {
        GridHadoopShuffleJob res = jobs.get(jobId);

        if (res == null) {
            res = new GridHadoopShuffleJob(ctx.jobTracker().job(jobId), mem,
                ctx.jobTracker().plan(jobId).reducers());

            GridHadoopShuffleJob old = jobs.putIfAbsent(jobId, res);

            if (old != null) {
                res.close();

                res = old;
            }
        }

        return res;
    }

    /**
     * @param taskInfo Task info.
     * @return Output.
     */
    public GridHadoopTaskOutput output(GridHadoopTaskInfo taskInfo) throws GridException {
        return job(taskInfo.jobId()).output(taskInfo);
    }

    /**
     * @param taskInfo Task info.
     * @return Input.
     */
    public GridHadoopTaskInput input(GridHadoopTaskInfo taskInfo) throws GridException {
        return job(taskInfo.jobId()).input(taskInfo);
    }

    /**
     * @param jobId Job id.
     */
    public void jobFinished(GridHadoopJobId jobId) {
        GridHadoopShuffleJob job = jobs.remove(jobId);

        if (job != null)
            job.close();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        for (GridHadoopShuffleJob job : jobs.values())
            job.close();

        jobs.clear();
    }
}
