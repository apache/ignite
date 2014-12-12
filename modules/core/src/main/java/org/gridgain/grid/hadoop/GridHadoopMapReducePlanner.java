/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Map-reduce execution planner.
 */
public interface GridHadoopMapReducePlanner {
    /**
     * Prepares map-reduce execution plan for the given job and topology.
     *
     * @param job Job.
     * @param top Topology.
     * @param oldPlan Old plan in case of partial failure.
     * @return Map reduce plan.
     */
    public GridHadoopMapReducePlan preparePlan(GridHadoopJob job, Collection<ClusterNode> top,
        @Nullable GridHadoopMapReducePlan oldPlan) throws IgniteCheckedException;
}
