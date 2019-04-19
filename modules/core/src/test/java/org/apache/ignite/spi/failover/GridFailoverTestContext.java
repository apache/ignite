/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.failover;

import java.util.List;
import java.util.Random;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;

/**
 * Failover test context.
 */
public class GridFailoverTestContext implements FailoverContext {
    /** */
    private static final Random RAND = new Random();

    /** Grid task session. */
    private final ComputeTaskSession taskSes;

    /** Failed job result. */
    private final ComputeJobResult jobRes;

    /** */
    public GridFailoverTestContext() {
        taskSes = null;
        jobRes = null;
    }

    /**
     * Initializes failover context.
     *
     * @param taskSes Grid task session.
     * @param jobRes Failed job result.
     */
    public GridFailoverTestContext(ComputeTaskSession taskSes, ComputeJobResult jobRes) {
        this.taskSes = taskSes;
        this.jobRes = jobRes;
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskSession getTaskSession() {
        return taskSes;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResult getJobResult() {
        return jobRes;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getBalancedNode(List<ClusterNode> grid) {
        return grid.get(RAND.nextInt(grid.size()));
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public String affinityCacheName() {
        return null;
    }
}