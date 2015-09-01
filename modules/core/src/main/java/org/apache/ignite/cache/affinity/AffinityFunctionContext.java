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

package org.apache.ignite.cache.affinity;

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity function context. This context is passed to {@link AffinityFunction} for
 * partition reassignment on every topology change event.
 */
public interface AffinityFunctionContext {
    /**
     * Gets affinity assignment for given partition on previous topology version. First node in returned list is
     * a primary node, other nodes are backups.
     *
     * @param part Partition to get previous assignment for.
     * @return List of nodes assigned to given partition on previous topology version or {@code null}
     *      if this information is not available.
     */
    @Nullable public List<ClusterNode> previousAssignment(int part);

    /**
     * Gets number of backups for new assignment.
     *
     * @return Number of backups for new assignment.
     */
    public int backups();

    /**
     * Gets current topology snapshot. Snapshot will contain only nodes on which particular cache is configured.
     * List of passed nodes is guaranteed to be sorted in a same order on all nodes on which partition assignment
     * is performed.
     *
     * @return Cache topology snapshot.
     */
    public List<ClusterNode> currentTopologySnapshot();

    /**
     * Gets current topology version number.
     *
     * @return Current topology version number.
     */
    public AffinityTopologyVersion currentTopologyVersion();

    /**
     * Gets discovery event caused topology change.
     *
     * @return Discovery event caused latest topology change or {@code null} if this information is
     *      not available.
     */
    @Nullable public DiscoveryEvent discoveryEvent();
}