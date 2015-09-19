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

package org.apache.ignite.internal.processors.hadoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobMetadata;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobTracker;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffle;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskExecutorAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Hadoop accelerator context.
 */
public class HadoopContext {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Hadoop configuration. */
    private HadoopConfiguration cfg;

    /** Job tracker. */
    private HadoopJobTracker jobTracker;

    /** External task executor. */
    private HadoopTaskExecutorAdapter taskExecutor;

    /** */
    private HadoopShuffle shuffle;

    /** Managers list. */
    private List<HadoopComponent> components = new ArrayList<>();

    /**
     * @param ctx Kernal context.
     */
    public HadoopContext(
        GridKernalContext ctx,
        HadoopConfiguration cfg,
        HadoopJobTracker jobTracker,
        HadoopTaskExecutorAdapter taskExecutor,
        HadoopShuffle shuffle
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
    public List<HadoopComponent> components() {
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
    public HadoopConfiguration configuration() {
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
        return ctx.discovery().cacheNodes(CU.SYS_CACHE_HADOOP_MR,
            new AffinityTopologyVersion(ctx.discovery().topologyVersion()));
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
    public boolean isParticipating(HadoopJobMetadata meta) {
        UUID locNodeId = localNodeId();

        if (locNodeId.equals(meta.submitNodeId()))
            return true;

        HadoopMapReducePlan plan = meta.mapReducePlan();

        return plan.mapperNodeIds().contains(locNodeId) || plan.reducerNodeIds().contains(locNodeId) || jobUpdateLeader();
    }

    /**
     * @return Jon tracker instance.
     */
    public HadoopJobTracker jobTracker() {
        return jobTracker;
    }

    /**
     * @return Task executor.
     */
    public HadoopTaskExecutorAdapter taskExecutor() {
        return taskExecutor;
    }

    /**
     * @return Shuffle.
     */
    public HadoopShuffle shuffle() {
        return shuffle;
    }

    /**
     * @return Map-reduce planner.
     */
    public HadoopMapReducePlanner planner() {
        return cfg.getMapReducePlanner();
    }

    /**
     * Adds component.
     *
     * @param c Component to add.
     * @return Added manager.
     */
    private <C extends HadoopComponent> C add(C c) {
        components.add(c);

        return c;
    }
}