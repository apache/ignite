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

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Hadoop accelerator context.
 */
public class GridHadoopContext {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Hadoop configuration. */
    private GridHadoopConfiguration cfg;

    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    /** External task executor. */
    private GridHadoopTaskExecutorAdapter taskExecutor;

    /** */
    private GridHadoopShuffle shuffle;

    /** Managers list. */
    private List<GridHadoopComponent> components = new ArrayList<>();

    /**
     * @param ctx Kernal context.
     */
    public GridHadoopContext(
        GridKernalContext ctx,
        GridHadoopConfiguration cfg,
        GridHadoopJobTracker jobTracker,
        GridHadoopTaskExecutorAdapter taskExecutor,
        GridHadoopShuffle shuffle
    ) {
        this.ctx = ctx;
        this.cfg = cfg;

        this.jobTracker = add(jobTracker);
        this.taskExecutor = add(taskExecutor);
        this.shuffle = add(shuffle);
    }

    /**
     * Gets list of managers.
     *
     * @return List of managers.
     */
    public List<GridHadoopComponent> components() {
        return components;
    }

    /**
     * Gets kernal context.
     *
     * @return Grid kernal context instance.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * Gets Hadoop configuration.
     *
     * @return Hadoop configuration.
     */
    public GridHadoopConfiguration configuration() {
        return cfg;
    }

    /**
     * Gets local node ID. Shortcut for {@code kernalContext().localNodeId()}.
     *
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return ctx.localNodeId();
    }

    /**
     * Gets local node order.
     *
     * @return Local node order.
     */
    public long localNodeOrder() {
        assert ctx.discovery() != null;

        return ctx.discovery().localNode().order();
    }

    /**
     * @return Hadoop-enabled nodes.
     */
    public Collection<ClusterNode> nodes() {
        return ctx.discovery().cacheNodes(CU.SYS_CACHE_HADOOP_MR, ctx.discovery().topologyVersion());
    }

    /**
     * @return {@code True} if
     */
    public boolean jobUpdateLeader() {
        long minOrder = Long.MAX_VALUE;
        ClusterNode minOrderNode = null;

        for (ClusterNode node : nodes()) {
            if (node.order() < minOrder) {
                minOrder = node.order();
                minOrderNode = node;
            }
        }

        assert minOrderNode != null;

        return localNodeId().equals(minOrderNode.id());
    }

    /**
     * @param meta Job metadata.
     * @return {@code true} If local node is participating in job execution.
     */
    public boolean isParticipating(GridHadoopJobMetadata meta) {
        UUID locNodeId = localNodeId();

        if (locNodeId.equals(meta.submitNodeId()))
            return true;

        GridHadoopMapReducePlan plan = meta.mapReducePlan();

        return plan.mapperNodeIds().contains(locNodeId) || plan.reducerNodeIds().contains(locNodeId) || jobUpdateLeader();
    }

    /**
     * @return Jon tracker instance.
     */
    public GridHadoopJobTracker jobTracker() {
        return jobTracker;
    }

    /**
     * @return Task executor.
     */
    public GridHadoopTaskExecutorAdapter taskExecutor() {
        return taskExecutor;
    }

    /**
     * @return Shuffle.
     */
    public GridHadoopShuffle shuffle() {
        return shuffle;
    }

    /**
     * @return Map-reduce planner.
     */
    public GridHadoopMapReducePlanner planner() {
        return cfg.getMapReducePlanner();
    }

    /**
     * Adds component.
     *
     * @param c Component to add.
     * @return Added manager.
     */
    private <C extends GridHadoopComponent> C add(C c) {
        components.add(c);

        return c;
    }
}
