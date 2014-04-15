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
    /** */
    private GridHadoopJobFactory jobFactory;

    /** */
    private GridHadoopMapReducePlanner planner;

    /**
     * Gets job factory.
     *
     * @return Job factory.
     */
    public GridHadoopJobFactory getJobFactory() {
        return jobFactory;
    }

    /**
     * Sets job factory.
     *
     * @param jobFactory Job factory.
     */
    public void setJobFactory(GridHadoopJobFactory jobFactory) {
        this.jobFactory = jobFactory;
    }

    /**
     * Gets map-reduce planner.
     *
     * @return Planner.
     */
    public GridHadoopMapReducePlanner getPlanner() {
        return planner;
    }

    /**
     * Sets map-reduce planner.
     *
     * @param planner Planner.
     */
    public void setPlanner(GridHadoopMapReducePlanner planner) {
        this.planner = planner;
    }
}
