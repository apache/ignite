/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;

/**
 * TODO write doc
 */
public interface GridHadoopPartitionerResolver {
    /**
     * Gets partitioner for the given job.
     *
     * @param jobInfo Job.
     * @return Partitioner.
     */
    public GridHadoopPartitioner partitioner(GridHadoopJobInfo jobInfo);
}
