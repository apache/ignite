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

package org.apache.ignite.internal.processors.metric.sources;

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.IntMetric;

/**
 * Metric source for cluster.
 */
public class ClusterMetricSource extends AbstractMetricSource<ClusterMetricSource.Holder> {
    /** Metrics source name. */
    public static final String CLUSTER_METRICS = "cluster";

    public ClusterMetricSource(GridKernalContext ctx) {
        super(CLUSTER_METRICS, ctx);
    }

    /**
     * Returns number of baseline nodes in a topology.
     *
     * @return Number of baseline nodes in a topology.
     */
    public int totalBaselineNodes() {
        return holder().bltNodes.value();
    }

    /**
     * Returns number of active baseline nodes in a topology.
     *
     * @return Number of active baseline nodes in a topology.
     */
    public int activeBaselineNodes() {
        return holder().activeBltNodes.value();
    }

    /**
     * Returns number of server nodes in a topology.
     *
     * @return Number of server nodes in a topology.
     */
    public int totalServerNodes() {
        return holder().srvNodes.value();
    }

    /**
     * Returns number of client nodes in a topology.
     *
     * @return Number of client nodes in a topology.
     */
    public int totalClientNodes() {
        return holder().clientNodes.value();
    }

    /**
     * Returns value indicating whether cluster is rebalacned or not.
     *
     * @return Boolean value indicating wether cluster is rebalacned or not.
     */
    public boolean rebalanced() {
        return holder().rebalanced.value();
    }

    /**
     * Sets value indicating whether cluster is rebalacned or not.
     *
     * @param val Value indicating whether cluster is rebalacned or not.
     */
    public void rebalanced(boolean val) {
        holder().rebalanced.value(val);
    }


    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        IgniteClusterImpl cluster = ctx().cluster().get();

        hldr.srvNodes = bldr.register("TotalServerNodes",
                () -> ctx().isStopping() || ctx().clientDisconnected() ? -1 : cluster.forServers().nodes().size(),
                "Number of server nodes in a topology.");

        hldr.clientNodes = bldr.register("TotalClientNodes",
                () -> ctx().isStopping() || ctx().clientDisconnected() ? -1 : cluster.forClients().nodes().size(),
                "Number of client nodes in a topology.");

        hldr.bltNodes = bldr.register("TotalBaselineNodes",
                () -> ctx().isStopping() || ctx().clientDisconnected() ? -1 : F.size(cluster.currentBaselineTopology()),
                "Number of baseline nodes in a topology.");

        hldr.activeBltNodes = bldr.register("ActiveBaselineNodes", () -> {
            if (ctx().isStopping() || ctx().clientDisconnected())
                return -1;

            Collection<Object> srvIds = F.nodeConsistentIds(cluster.forServers().nodes());

            return F.size(cluster.currentBaselineTopology(), node -> srvIds.contains(node.consistentId()));
        }, "Number of active baseline nodes in a topology.");

        hldr.rebalanced = bldr.booleanMetric("Rebalanced",
                "True if the cluster has achieved fully rebalanced state. Note that an inactive cluster always has" +
                        " this metric in False regardless of the real partitions state.");

    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Total server nodes count metric. */
        private IntMetric srvNodes;

        /** Total client nodes count metric. */
        private IntMetric clientNodes;

        /** Total baseline nodes count metric. */
        private IntMetric bltNodes;

        /** Active baseline nodes count metric. */
        private IntMetric activeBltNodes;

        /** Metric that shows whether cluster is in fully rebalanced state. */
        private BooleanMetricImpl rebalanced;
    }
}
