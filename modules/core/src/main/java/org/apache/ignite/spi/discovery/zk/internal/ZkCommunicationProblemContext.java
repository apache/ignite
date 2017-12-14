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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CommunicationProblemContext;

/**
 *
 */
class ZkCommunicationProblemContext implements CommunicationProblemContext {
    /** */
    private Set<ClusterNode> killedNodes = new HashSet<>();

    /** {@inheritDoc} */
    @Override public List<ClusterNode> topologySnapshot() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean connectionAvailable(ClusterNode node1, ClusterNode node2) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public List<String> startedCaches() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> cacheAffinity(String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> cachePartitionOwners(String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void killNode(ClusterNode node) {
        if (node == null)
            throw new NullPointerException();

        killedNodes.add(node);
    }
}
