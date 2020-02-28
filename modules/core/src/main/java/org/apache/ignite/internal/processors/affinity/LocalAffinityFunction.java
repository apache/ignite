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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Affinity function for {@link org.apache.ignite.cache.CacheMode#LOCAL} caches.
 */
public class LocalAffinityFunction implements AffinityFunction {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        ClusterNode locNode = null;

        for (ClusterNode n : affCtx.currentTopologySnapshot()) {
            if (n.isLocal()) {
                locNode = n;

                break;
            }
        }

        if (locNode == null)
            throw new IgniteException("Local node is not included into affinity nodes for 'LOCAL' cache");

        List<List<ClusterNode>> res = new ArrayList<>(partitions());

        for (int part = 0; part < partitions(); part++)
            res.add(Collections.singletonList(locNode));

        return Collections.unmodifiableList(res);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }
}
