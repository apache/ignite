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

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MappingServiceImpl implements MappingService {
    /**
     *
     */
    private final TopologyService topSrvc;

    public MappingServiceImpl(TopologyService topSrvc) {
        this.topSrvc = topSrvc;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> executionNodes(long topVer, boolean single, @Nullable Predicate<ClusterNode> nodeFilter) {
        List<ClusterNode> nodes = new ArrayList<>(topSrvc.allMembers());

        if (nodeFilter != null) {
            nodes = nodes.stream().filter(nodeFilter).collect(Collectors.toList());
        }

        if (single && nodes.size() > 1) {
            nodes = singletonList(nodes.get(ThreadLocalRandom.current().nextInt(nodes.size())));
        }

        if (nullOrEmpty(nodes)) {
            throw new IllegalStateException("failed to map query to execution nodes. Nodes list is empty.");
        }

        return Commons.transform(nodes, ClusterNode::id);
    }
}
