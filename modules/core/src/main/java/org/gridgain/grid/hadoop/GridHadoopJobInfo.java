/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Compact job description.
 */
public interface GridHadoopJobInfo extends Serializable {
    /**
     * Gets optional configuration property for the job.
     *
     * @param name Property name.
     * @return Value or {@code null} if none.
     */
    @Nullable public String property(String name);

    /**
     * Checks whether job has combiner.
     *
     * @return {@code true} If job has combiner.
     */
    public boolean hasCombiner();

    /**
     * Checks whether job has reducer.
     * Actual number of reducers will be in {@link GridHadoopMapReducePlan#reducers()}.
     *
     * @return Number of reducer.
     */
    public boolean hasReducer();

    /**
     * Creates new job instance for the given ID.
     * {@link GridHadoopJobInfo} is reusable for multiple jobs while {@link GridHadoopJob} is for one job execution.
     * This method will be called once for the same ID on one node, though it can be called on the same host
     * multiple times from different processes (in case of multiple nodes on the same host or external execution).
     *
     * @param jobId Job ID.
     * @param log Logger.
     * @return Job.
     * @throws GridException If failed.
     */
    GridHadoopJob createJob(GridHadoopJobId jobId, GridLogger log) throws GridException;

    /**
     * @return Number of reducers configured for job.
     */
    public int reducers();

    /**
     * Gets job name.
     *
     * @return Job name.
     */
    public String jobName();

    /**
     * Gets user name.
     *
     * @return User name.
     */
    public String user();
}
