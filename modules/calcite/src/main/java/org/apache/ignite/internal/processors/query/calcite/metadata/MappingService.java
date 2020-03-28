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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.function.Predicate;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.util.Service;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service is responsible for nodes mapping calculation.
 */
public interface MappingService extends Service {
    /**
     * Returns Nodes mapping for intermediate fragments, without Scan nodes leafs. Such fragments may be executed
     * on any cluster node, actual list of nodes is chosen on the basis of adopted selection strategy (using node filter).
     *
     * @param topVer Topology version.
     * @param desiredCnt desired nodes count, {@code 0} means all possible nodes.
     * @param nodeFilter Node filter.
     * @return Nodes mapping for intermediate fragments.
     */
    NodesMapping mapBalanced(@NotNull AffinityTopologyVersion topVer, int desiredCnt, @Nullable Predicate<ClusterNode> nodeFilter);
}
