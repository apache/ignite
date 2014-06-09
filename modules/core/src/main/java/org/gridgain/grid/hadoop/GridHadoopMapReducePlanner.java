/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Map-reduce execution planner.
 */
public interface GridHadoopMapReducePlanner {
    /**
     * Prepares map-reduce execution plan for the given collection of input splits and topology.
     *
     * @param splits Input splits.
     * @param top Topology.
     * @param job Job.
     * @param oldPlan Old plan in case of partial failure.
     * @return Map reduce plan.
     */
    public GridHadoopMapReducePlan preparePlan(Collection<GridHadoopInputSplit> splits, Collection<GridNode> top,
        GridHadoopJob job, @Nullable GridHadoopMapReducePlan oldPlan) throws GridException;
}
