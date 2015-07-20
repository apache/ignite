/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.failover;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;

import java.util.*;

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
    @Override public Object affinityKey() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String affinityCacheName() {
        return null;
    }
}
