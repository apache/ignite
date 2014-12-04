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
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopClassLoader.*;

/**
 * Hadoop processor.
 */
public class GridHadoopProcessor extends GridHadoopProcessorAdapter {
    /** Job ID counter. */
    private final AtomicInteger idCtr = new AtomicInteger();

    /** Hadoop context. */
    @GridToStringExclude
    private GridHadoopContext hctx;

    /** Hadoop facade for public API. */
    @GridToStringExclude
    private GridHadoop hadoop;

    /**
     * @param ctx Kernal context.
     */
    public GridHadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.isDaemon())
            return;

        GridHadoopConfiguration cfg = ctx.config().getHadoopConfiguration();

        if (cfg == null)
            cfg = new GridHadoopConfiguration();
        else
            cfg = new GridHadoopConfiguration(cfg);

        initializeDefaults(cfg);

        validate(cfg);

        if (hadoopHome() != null)
            U.quietAndInfo(log, "HADOOP_HOME is set to " + hadoopHome());

        boolean ok = false;

        try { // Check for Hadoop installation.
            hadoopUrls();

            ok = true;
        }
        catch (GridException e) {
            U.quietAndWarn(log, e.getMessage());
        }

        if (ok) {
            hctx = new GridHadoopContext(
                ctx,
                cfg,
                new GridHadoopJobTracker(),
                cfg.isExternalExecution() ? new GridHadoopExternalTaskExecutor() : new GridHadoopEmbeddedTaskExecutor(),
                new GridHadoopShuffle());


            for (GridHadoopComponent c : hctx.components())
                c.start(hctx);

            hadoop = new GridHadoopImpl(this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopProcessor.class, this);
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
        if (hadoop == null)
            throw new IllegalStateException("Hadoop accelerator is disabled (Hadoop is not in classpath, " +
                "is HADOOP_HOME environment variable set?)");

        return hadoop;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration config() {
        return hctx.configuration();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId nextJobId() {
        return new GridHadoopJobId(ctx.localNodeId(), idCtr.incrementAndGet());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return hctx.jobTracker().submit(jobId, jobInfo);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException {
        return hctx.jobTracker().status(jobId);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounters counters(GridHadoopJobId jobId) throws GridException {
        return hctx.jobTracker().jobCounters(jobId);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> finishFuture(GridHadoopJobId jobId) throws GridException {
        return hctx.jobTracker().finishFuture(jobId);
    }

    /** {@inheritDoc} */
    @Override public boolean kill(GridHadoopJobId jobId) throws GridException {
        return hctx.jobTracker().killJob(jobId);
    }

    /**
     * Initializes default hadoop configuration.
     *
     * @param cfg Hadoop configuration.
     */
    private void initializeDefaults(GridHadoopConfiguration cfg) {
        if (cfg.getMapReducePlanner() == null)
            cfg.setMapReducePlanner(new GridHadoopDefaultMapReducePlanner());
    }

    /**
     * Validates Grid and Hadoop configuration for correctness.
     *
     * @param hadoopCfg Hadoop configuration.
     * @throws GridException If failed.
     */
    private void validate(GridHadoopConfiguration hadoopCfg) throws GridException {
        if (ctx.config().isPeerClassLoadingEnabled())
            throw new GridException("Peer class loading cannot be used with Hadoop (disable it using " +
                "GridConfiguration.setPeerClassLoadingEnabled()).");
    }
}
