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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;

/**
 * Platform AffinityFunction.
 */
public class PlatformAffinityFunction implements AffinityFunction, Externalizable {
    private final AffinityFunction aff = new FairAffinityFunction();

    private int cnt;

    @Override public void reset() {
        aff.reset();
    }

    @Override public int partitions() {
        return aff.partitions();
    }

    @Override public int partition(Object key) {
        return aff.partition(key);
    }

    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        return aff.assignPartitions(affCtx);
    }

    @Override public void removeNode(UUID nodeId) {
        aff.removeNode(nodeId);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    @IgniteInstanceResource
    private void setIgnite(Ignite ignite) {
        cnt++;

        if (cnt > 1) {
            assert  ignite != null;
        }
    }
}
