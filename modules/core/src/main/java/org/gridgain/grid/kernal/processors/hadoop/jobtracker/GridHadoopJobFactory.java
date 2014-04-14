/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

/**
 * Job factory.
 */
public interface GridHadoopJobFactory<T> {
    /**
     * Creates job.
     *
     * @param id Job ID.
     * @param jobInfo Job information.
     * @return Job instance.
     */
    public GridHadoopJob<T> createJob(GridHadoopJobId id, GridHadoopJobInfo<T> jobInfo);
}
