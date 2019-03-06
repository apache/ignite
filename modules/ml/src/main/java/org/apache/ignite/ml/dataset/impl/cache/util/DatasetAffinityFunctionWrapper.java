/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
