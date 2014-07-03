/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Hadoop configuration.
 */
public class GridHadoopConfiguration {
    /** Default finished job info time-to-live. */
    public static final long DFLT_FINISHED_JOB_INFO_TTL = 10_000;

    /** Default value for external execution flag. */
    public static final boolean DFLT_EXTERNAL_EXECUTION = false;

    /** Map reduce planner. */
    private GridHadoopMapReducePlanner planner;

    /** */
    private boolean extExecution = DFLT_EXTERNAL_EXECUTION;

    /** Finished job info TTL. */
    private long finishedJobInfoTtl = DFLT_FINISHED_JOB_INFO_TTL;

    /** */
    private ExecutorService embeddedExecutor;

    /**
     * Default constructor.
     */
    public GridHadoopConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridHadoopConfiguration(GridHadoopConfiguration cfg) {
        // Preserve alphabetic order.
        embeddedExecutor = cfg.getEmbeddedExecutor();
        extExecution = cfg.isExternalExecution();
        finishedJobInfoTtl = cfg.getFinishedJobInfoTtl();
        planner = cfg.getMapReducePlanner();
    }

    /**
     * Sets executor service to run task in embedded mode.
     * Embedded mode means that {@linkplain #isExternalExecution()} is {@code false}.
     *
     * @param embeddedExecutor Executor service.
     */
    public void setEmbeddedExecutor(ExecutorService embeddedExecutor) {
        this.embeddedExecutor = embeddedExecutor;
    }

    /**
     * Gets executor service to run task in embedded mode.
     * Embedded mode means that {@linkplain #isExternalExecution()} is {@code false}.
     *
     * @return Executor service.
     */
    public ExecutorService getEmbeddedExecutor() {
        return embeddedExecutor;
    }

    /**
     * Gets finished job info time-to-live in milliseconds.
     *
     * @return Finished job info time-to-live.
     */
    public long getFinishedJobInfoTtl() {
        return finishedJobInfoTtl;
    }

    /**
     * Sets finished job info time-to-live.
     *
     * @param finishedJobInfoTtl Finished job info time-to-live.
     */
    public void setFinishedJobInfoTtl(long finishedJobInfoTtl) {
        this.finishedJobInfoTtl = finishedJobInfoTtl;
    }

    /**
     * Gets external task execution flag. If {@code true}, hadoop job tasks will be executed in an external
     * (relative to node) process.
     *
     * @return {@code True} if external execution.
     */
    public boolean isExternalExecution() {
        return extExecution;
    }

    /**
     * Sets external task execution flag.
     *
     * @param extExecution {@code True} if tasks should be executed in an external process.
     * @see #isExternalExecution()
     */
    public void setExternalExecution(boolean extExecution) {
        this.extExecution = extExecution;
    }

    /**
     * Gets Hadoop map-reduce planner, a component which defines job execution plan based on job
     * configuration and current grid topology.
     *
     * @return Map-reduce planner.
     */
    public GridHadoopMapReducePlanner getMapReducePlanner() {
        return planner;
    }

    /**
     * Sets Hadoop map-reduce planner, a component which defines job execution plan based on job
     * configuration and current grid topology.
     *
     * @param planner Map-reduce planner.
     */
    public void setMapReducePlanner(GridHadoopMapReducePlanner planner) {
        this.planner = planner;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopConfiguration.class, this, super.toString());
    }
}
