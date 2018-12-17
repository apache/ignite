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

package org.apache.ignite.tensorflow.cluster.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;

/**
 * TensorFlow cluster resolver based on Ignite Cache affinity.
 */
public class TensorFlowClusterResolver {
    /** TensorFlow worker job name. */
    public static final String WORKER_JOB_NAME = "worker";

    /** TensorFlow chief job name. */
    public static final String CHIEF_JOB_NAME = "chief";

    /** Ignite instance. */
    private final Ignite ignite;

    /** Cluster port manager. */
    private final ClusterPortManager portMgr;

    /**
     * Constructs a new instance of TensorFlow cluster resolver.
     *
     * @param ignite Ignite instance.
     */
    public TensorFlowClusterResolver(Ignite ignite, String portPoolName, int portFrom, int portCnt) {
        assert ignite != null : "Ignite instance should not be null";
        assert portPoolName != null : "Port pool name should not be null";
        assert portFrom >= 0 : "Port count should not be negative";
        assert portCnt >= 0 && portCnt + portFrom <= 0xFFFF : "Port range should be between 0 and 65535";

        this.ignite = ignite;
        this.portMgr = new ClusterPortManager(ignite, portPoolName, portFrom, portCnt);
    }

    /**
     * Resolves TensorFlow cluster and acquires required ports.
     *
     * @param upstreamCacheName Upstream cache name.
     * @return TensorFlow cluster specification.
     */
    public TensorFlowClusterSpec resolveAndAcquirePorts(String upstreamCacheName) {
        TensorFlowClusterSpec spec = new TensorFlowClusterSpec();

        resolveAndAcquirePortsForWorkers(spec, upstreamCacheName);
        resolveAndAcquirePortsForChief(spec);

        return spec;
    }

    /**
     * Releases ports acquired for the given cluster specification.
     *
     * @param spec TensorFlow cluster specification.
     */
    public void releasePorts(TensorFlowClusterSpec spec) {
        for (String jobName : spec.getJobs().keySet())
            for (TensorFlowServerAddressSpec address : spec.getJobs().get(jobName))
                portMgr.releasePort(address.getNodeId(), address.getPort());
    }

    /** Destroys TensorFlow cluster resolver. */
    public void destroy() {
        portMgr.destroy();
    }

    /**
     * Resolves TensorFlow cluster worker jobs and acquires ports.
     *
     * @param spec TensorFlow cluster specification.
     * @param upstreamCacheName Upstream cache name.
     */
    private void resolveAndAcquirePortsForWorkers(TensorFlowClusterSpec spec, String upstreamCacheName) {
        Affinity<?> affinity = ignite.affinity(upstreamCacheName);
        int parts = affinity.partitions();

        Set<UUID> distinctNodeIds = new HashSet<>();
        for (int part = 0; part < parts; part++) {
            ClusterNode node = affinity.mapPartitionToNode(part);
            UUID nodeId = node.id();
            distinctNodeIds.add(nodeId);
        }
        List<UUID> nodeIds = new ArrayList<>(distinctNodeIds);
        Collections.sort(nodeIds);

        for (UUID nodeId : nodeIds) {
            int port = portMgr.acquirePort(nodeId);
            spec.addTask(WORKER_JOB_NAME, nodeId, port);
        }
    }

    /**
     * Resolves TensorFlow cluster chief job and acquires ports.
     *
     * @param spec TensorFlow cluster specification.
     */
    private void resolveAndAcquirePortsForChief(TensorFlowClusterSpec spec) {
        ClusterNode chiefNode = ignite.cluster().localNode();
        UUID chiefNodeId = chiefNode.id();
        int chiefPort = portMgr.acquirePort(chiefNodeId);

        spec.addTask(CHIEF_JOB_NAME, chiefNodeId, chiefPort);
    }
}
