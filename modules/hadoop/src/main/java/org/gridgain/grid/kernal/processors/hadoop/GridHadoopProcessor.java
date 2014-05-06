/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.kernal.processors.hadoop.planner.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Hadoop processor.
 */
public class GridHadoopProcessor extends GridHadoopProcessorAdapter {
    /** Unique ID of this processor. */
    private final UUID id = UUID.randomUUID();

    /** Job ID counter. */
    private final AtomicInteger idCtr = new AtomicInteger();

    /** Hadoop context. */
    private GridHadoopContext hctx;

    /** Hadoop facade for public API. */
    private GridHadoop hadoop;

    /**
     * @param ctx Kernal context.
     */
    public GridHadoopProcessor(GridKernalContext ctx) {
        super(ctx);


    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        super.start();

        GridHadoopConfiguration cfg = ctx.config().getHadoopConfiguration();

        if (ctx.isDaemon() || cfg == null)
            return;

        // Make copy.
        cfg = new GridHadoopConfiguration(cfg);

        initializeDefaults(cfg);

        validate(cfg);

        hctx = new GridHadoopContext(ctx,
            cfg,
            new GridHadoopJobTracker(),
            new GridHadoopTaskExecutor(),
            new GridHadoopShuffle());

        for (GridHadoopComponent c : hctx.components())
            c.start(hctx);

        hadoop = new GridHadoopImpl(this);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        super.stop(cancel);

        if (hctx == null)
            return;

        List<GridHadoopComponent> components = hctx.components();

        for (ListIterator<GridHadoopComponent> it = components.listIterator(components.size()); it.hasPrevious();) {
            GridHadoopComponent c = it.previous();

            c.stop(cancel);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        if (hctx == null)
            return;

        for (GridHadoopComponent c : hctx.components())
            c.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (hctx == null)
            return;

        List<GridHadoopComponent> components = hctx.components();

        for (ListIterator<GridHadoopComponent> it = components.listIterator(components.size()); it.hasPrevious();) {
            GridHadoopComponent c = it.previous();

            c.onKernalStop(cancel);
        }
    }

    /**
     * Gets Hadoop context.
     *
     * @return Hadoop context.
     */
    public GridHadoopContext context() {
        return hctx;
    }

    /** {@inheritDoc} */
    @Override public GridHadoop hadoop() {
        return hadoop;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration config() {
        return hctx.configuration();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId nextJobId() {
        return new GridHadoopJobId(id, idCtr.incrementAndGet());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return hctx.jobTracker().submit(jobId, jobInfo);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException {
        return hctx.jobTracker().status(jobId);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> finishFuture(GridHadoopJobId jobId) throws GridException {
        return hctx.jobTracker().finishFuture(jobId);
    }

    /**
     * Initializes default hadoop configuration.
     *
     * @param cfg Hadoop configuration.
     */
    private void initializeDefaults(GridHadoopConfiguration cfg) {
        if (cfg.getMapReducePlanner() == null)
            cfg.setMapReducePlanner(new GridHadoopDefaultMapReducePlanner());

        if (cfg.getJobFactory() == null)
            cfg.setJobFactory(new GridHadoopDefaultJobFactory());
    }

    /**
     * Validates hadoop configuration for correctness.
     *
     * @param cfg Hadoop configuration.
     */
    private void validate(GridHadoopConfiguration cfg) {

    }
}
