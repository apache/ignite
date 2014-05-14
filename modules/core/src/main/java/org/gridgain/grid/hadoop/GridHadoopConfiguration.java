/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

/**
 * Hadoop configuration.
 */
public class GridHadoopConfiguration {
    /** Default finished jbo info time-to-live. */
    public static final long DFLT_FINISHED_JOB_INFO_TTL = 10_000;

    /** System cache name. TODO get rid of it. */
    private String sysCacheName;

    /** Job factory. */
    private GridHadoopJobFactory jobFactory;

    /** Map reduce planner. */
    private GridHadoopMapReducePlanner planner;

    /** Finished job info TTL. */
    private long finishedJobInfoTtl = DFLT_FINISHED_JOB_INFO_TTL;

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
        finishedJobInfoTtl = cfg.getFinishedJobInfoTtl();
        jobFactory = cfg.getJobFactory();
        planner = cfg.getMapReducePlanner();
        sysCacheName = cfg.getSystemCacheName();
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
     * @return TODO remove.
     */
    public String getSystemCacheName() {
        return sysCacheName;
    }

    /**
     * @param sysCacheName TODO remove.
     */
    public void setSystemCacheName(String sysCacheName) {
        this.sysCacheName = sysCacheName;
    }

    /**
     * Gets Hadoop job factory.
     *
     * @return Hadoop job factory.
     */
    public GridHadoopJobFactory getJobFactory() {
        return jobFactory;
    }

    /**
     * Sets Hadoop job factory.
     *
     * @param jobFactory Job factory.
     */
    public void setJobFactory(GridHadoopJobFactory jobFactory) {
        this.jobFactory = jobFactory;
    }
}
