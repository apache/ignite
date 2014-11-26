/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Hadoop configuration.
 */
public class GridHadoopConfiguration {
    /** Default finished job info time-to-live. */
    public static final long DFLT_FINISHED_JOB_INFO_TTL = 10_000;

    /** Default value for external execution flag. */
    public static final boolean DFLT_EXTERNAL_EXECUTION = false;

    /** Default value for the max parallel tasks. */
    public static final int DFLT_MAX_PARALLEL_TASKS = Runtime.getRuntime().availableProcessors();

    /** Default value for the max task queue size. */
    public static final int DFLT_MAX_TASK_QUEUE_SIZE = 1000;

    /** Map reduce planner. */
    private GridHadoopMapReducePlanner planner;

    /** */
    private boolean extExecution = DFLT_EXTERNAL_EXECUTION;

    /** Finished job info TTL. */
    private long finishedJobInfoTtl = DFLT_FINISHED_JOB_INFO_TTL;

    /** */
    private int maxParallelTasks = DFLT_MAX_PARALLEL_TASKS;

    /** */
    private int maxTaskQueueSize = DFLT_MAX_TASK_QUEUE_SIZE;

    /** */
    private GridHadoopStatWriter statWriter;

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
        extExecution = cfg.isExternalExecution();
        finishedJobInfoTtl = cfg.getFinishedJobInfoTtl();
        planner = cfg.getMapReducePlanner();
        maxParallelTasks = cfg.getMaxParallelTasks();
        maxTaskQueueSize = cfg.getMaxTaskQueueSize();
        statWriter = cfg.getStatWriter();
    }

    /**
     * Gets max number of local tasks that may be executed in parallel.
     *
     * @return Max number of local tasks that may be executed in parallel.
     */
    public int getMaxParallelTasks() {
        return maxParallelTasks;
    }

    /**
     * Sets max number of local tasks that may be executed in parallel.
     *
     * @param maxParallelTasks Max number of local tasks that may be executed in parallel.
     */
    public void setMaxParallelTasks(int maxParallelTasks) {
        this.maxParallelTasks = maxParallelTasks;
    }

    /**
     * Gets max task queue size.
     *
     * @return Max task queue size.
     */
    public int getMaxTaskQueueSize() {
        return maxTaskQueueSize;
    }

    /**
     * Sets max task queue size.
     *
     * @param maxTaskQueueSize Max task queue size.
     */
    public void setMaxTaskQueueSize(int maxTaskQueueSize) {
        this.maxTaskQueueSize = maxTaskQueueSize;
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

    /**
     * Gets writer for job statistics.
     *
     * @return Statistics writer.
     */
    public GridHadoopStatWriter getStatWriter() {
        return statWriter;
    }

    /**
     * Sets writer for job statistics.
     *
     * @param statWriter Statistics writer.
     */
    public void setStatWriter(GridHadoopStatWriter statWriter) {
        this.statWriter = statWriter;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopConfiguration.class, this, super.toString());
    }
}
