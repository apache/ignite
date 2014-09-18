/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Map-reduce job execution plan.
 */
public interface GridHadoopMapReducePlan extends Serializable {
    /**
     * Gets collection of file blocks for which mappers should be executed.
     *
     * @param nodeId Node ID to check.
     * @return Collection of file blocks or {@code null} if no mappers should be executed on given node.
     */
    @Nullable public Collection<GridHadoopInputSplit> mappers(UUID nodeId);

    /**
     * Gets reducer IDs that should be started on given node.
     *
     * @param nodeId Node ID to check.
     * @return Array of reducer IDs.
     */
    @Nullable public int[] reducers(UUID nodeId);

    /**
     * Gets collection of all node IDs involved in map part of job execution.
     *
     * @return Collection of node IDs.
     */
    public Collection<UUID> mapperNodeIds();

    /**
     * Gets collection of all node IDs involved in reduce part of job execution.
     *
     * @return Collection of node IDs.
     */
    public Collection<UUID> reducerNodeIds();

    /**
     * Gets overall number of mappers for the job.
     *
     * @return Number of mappers.
     */
    public int mappers();

    /**
     * Gets overall number of reducers for the job.
     *
     * @return Number of reducers.
     */
    public int reducers();

    /**
     * Gets node ID for reducer.
     *
     * @param reducer Reducer.
     * @return Node ID.
     */
    public UUID nodeForReducer(int reducer);
}
