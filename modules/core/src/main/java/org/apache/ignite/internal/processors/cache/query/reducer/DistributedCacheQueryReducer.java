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

package org.apache.ignite.internal.processors.cache.query.reducer;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Base class for distributed cache query reducers.
 */
public abstract class DistributedCacheQueryReducer<T> extends CacheQueryReducer<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Function returns a new data page for specified node.
     */
    protected final Function<UUID, NodePage<T>> pagesProvider;

    /**
     * List of nodes with not reduced data.
     */
    protected final List<UUID> nodes;

    /** */
    protected DistributedCacheQueryReducer(Function<UUID, NodePage<T>> pagesProvider, Collection<ClusterNode> nodes) {
        this.pagesProvider = pagesProvider;
        this.nodes = nodes.stream().map(ClusterNode::id).collect(Collectors.toList());
    }
}
