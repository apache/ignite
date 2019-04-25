/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Affinity function wrapper that uses key as a partition index and delegates all other functions to specified
 * delegate.
 */
public class DatasetAffinityFunctionWrapper implements AffinityFunction {
    /** */
    private static final long serialVersionUID = -8233787063079973753L;

    /** Delegate that actually performs all methods except {@code partition()}. */
    private final AffinityFunction delegate;

    /**
     * Constructs a new instance of affinity function wrapper.
     *
     * @param delegate Affinity function which actually performs all methods except {@code partition()}.
     */
    public DatasetAffinityFunctionWrapper(AffinityFunction delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        delegate.reset();
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return delegate.partitions();
    }

    /**
     * Returns key as a partition index.
     *
     * @param key Partition index.
     * @return Partition index.
     */
    @Override public int partition(Object key) {
        return (Integer) key;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        return delegate.assignPartitions(affCtx);
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        delegate.removeNode(nodeId);
    }
}
