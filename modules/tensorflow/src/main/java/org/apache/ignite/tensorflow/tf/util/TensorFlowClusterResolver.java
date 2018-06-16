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

package org.apache.ignite.tensorflow.tf.util;

import org.apache.ignite.tensorflow.tf.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.tf.spec.TensorFlowServerAddressSpec;
import java.util.UUID;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;

/**
 * TensorFlow cluster resolver based on Ignite Cache affinity.
 */
public class TensorFlowClusterResolver implements Component {
    /** */
    private static final long serialVersionUID = 8881305616566367624L;

    /** Cluster port manager. */
    private final ClusterPortManager portMgr = new ClusterPortManager("TF_POOL", 10000, 100);

    /** {@inheritDoc} */
    @Override public void init() {
        portMgr.init();
    }

    /**
     * Resolves TensorFlow cluster and acquires required ports.
     *
     * @param upstreamCacheName Upstream cache name.
     * @return TensorFlow cluster specification.
     */
    public TensorFlowClusterSpec resolveAndAcquirePorts(String upstreamCacheName) {
        Affinity<?> affinity = Ignition.ignite().affinity(upstreamCacheName);

        int parts = affinity.partitions();

        TensorFlowClusterSpec spec = new TensorFlowClusterSpec();

        for (int part = 0; part < parts; part++) {
            ClusterNode node = affinity.mapPartitionToNode(part);
            UUID nodeId = node.id();

            int port = portMgr.acquirePort(nodeId);

            spec.addTask("WORKER", nodeId, port);
        }

        return spec;
    }

    /**
     * Frees ports acquired for the given cluster specification.
     *
     * @param spec TensorFlow cluster specification.
     */
    public void freePorts(TensorFlowClusterSpec spec) {
        for (String jobName : spec.getJobs().keySet())
            for (TensorFlowServerAddressSpec address : spec.getJobs().get(jobName))
                portMgr.freePort(address.getNodeId(), address.getPort());
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        portMgr.destroy();
    }
}
