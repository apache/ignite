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

package org.apache.ignite.internal.processors.platform.cache.affinity;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;

import java.util.List;
import java.util.UUID;

/**
 * Platform AffinityFunction.
 */
public class PlatformAffinityFunction implements AffinityFunction {
    private final AffinityFunction _aff = new FairAffinityFunction();

    @Override public void reset() {
        _aff.reset();
    }

    @Override public int partitions() {
        return _aff.partitions();
    }

    @Override public int partition(Object key) {
        return _aff.partition(key);
    }

    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        return _aff.assignPartitions(affCtx);
    }

    @Override public void removeNode(UUID nodeId) {
        _aff.removeNode(nodeId);
    }
}
