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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MappingServiceImpl extends AbstractService implements MappingService {
    /**
     * Max nodes count, used on to-hash or to-random redistribution.
     */
    private static final int MAX_BUCKETS_COUNT = IgniteSystemProperties.getInteger("IGNITE_CALCITE_MAX_BUCKETS_COUNT", 1024);

    /** */
    private GridDiscoveryManager discoveryManager;

    /**
     * @param ctx Kernal.
     */
    public MappingServiceImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param discoveryManager Discovery manager.
     */
    public void discoveryManager(GridDiscoveryManager discoveryManager) {
        this.discoveryManager = discoveryManager;
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        discoveryManager(ctx.discovery());
    }

    /** {@inheritDoc} */
    @Override public NodesMapping mapBalanced(@NotNull AffinityTopologyVersion topVer, int desiredCnt, @Nullable Predicate<ClusterNode> nodeFilter) {
        assert desiredCnt >= 0;

        desiredCnt = desiredCnt == 0 ? MAX_BUCKETS_COUNT : Math.min(desiredCnt, MAX_BUCKETS_COUNT);

        List<ClusterNode> nodes = new ArrayList<>(discoveryManager.discoCache(topVer).serverNodes());

        if (nodeFilter != null)
            nodes = nodes.stream().filter(nodeFilter).collect(Collectors.toList());

        if (desiredCnt < nodes.size()) {
            Collections.shuffle(nodes);

            nodes = nodes.subList(0, desiredCnt);
        }

        return new NodesMapping(Commons.transform(nodes, ClusterNode::id), null, (byte) 0);
    }
}
