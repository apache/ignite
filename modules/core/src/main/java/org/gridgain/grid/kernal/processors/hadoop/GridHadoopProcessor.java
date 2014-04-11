/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Hadoop processor.
 */
public class GridHadoopProcessor extends GridProcessorAdapter {
    /** Hadoop context. */
    private GridHadoopContext hctx;

    /**
     * @param ctx Kernal context.
     */
    protected GridHadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        super.start();

        if (ctx.isDaemon() || ctx.config().getHadoopConfiguration() == null)
            return;

        hctx = new GridHadoopContext(
            ctx,
            new GridHadoopJobTrackerManager());

        for (GridHadoopManager mgr : hctx.managers())
            mgr.start(hctx);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        super.stop(cancel);

        if (hctx == null)
            return;

        List<GridHadoopManager> mgrs = hctx.managers();

        for (ListIterator<GridHadoopManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
            GridHadoopManager mgr = it.previous();

            mgr.stop(cancel);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        if (hctx == null)
            return;

        for (GridHadoopManager mgr : hctx.managers())
            mgr.onKernalStart();

    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (hctx == null)
            return;

        List<GridHadoopManager> mgrs = hctx.managers();

        for (ListIterator<GridHadoopManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
            GridHadoopManager mgr = it.previous();

            mgr.onKernalStop(cancel);
        }
    }

    /**
     * @param cnt Number of IDs to generate.
     * @return Collection of generated IDs.
     */
    public Collection<GridHadoopJobId> getNextJobIds(int cnt) {
        return null;
    }

    /**
     * Submits job to job tracker.
     *
     * @param jobId Job ID to submit.
     * @param jobInfo Job info to submit.
     * @return Execution future.
     */
    public GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return null;
    }

    /**
     * Gets hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status.
     */
    @Nullable public GridHadoopJobStatus status(GridHadoopJobId jobId) {
        return null;
    }
}
