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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Builder that accepts a partition {@code upstream} data and partition {@code context} and makes partition
 * {@code data}. This builder is used to build a partition {@code data} and assumed to be called in all cases when
 * partition {@code data} not found on the node that performs computation (it might be the result of a previous node
 * failure or rebalancing).
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 * @param <D> Type of a partition <tt>data</tt>.
 * @see SimpleDatasetDataBuilder
 * @see SimpleLabeledDatasetDataBuilder
 */
@FunctionalInterface
public interface PartitionDataBuilder<K, V, C extends Serializable, D extends AutoCloseable> extends Serializable {
    /**
     * Builds a new partition {@code data} from a partition {@code upstream} data and partition {@code context}
     *
     * @param upstreamData Partition {@code upstream} data.
     * @param upstreamDataSize Partition {@code upstream} data size.
     * @param ctx Partition {@code context}.
     * @return Partition {@code data}.
     */
    public D build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize, C ctx);

    /**
     * Makes a composed partition {@code data} builder that first builds a {@code data} and then applies the specified
     * function on the result.
     *
     * @param fun Function that applied after first partition {@code data} is built.
     * @param <D2> New type of a partition {@code data}.
     * @return Composed partition {@code data} builder.
     */
    default public <D2 extends AutoCloseable> PartitionDataBuilder<K, V, C, D2> andThen(
        IgniteBiFunction<D, C, D2> fun) {
        return (upstreamData, upstreamDataSize, ctx) -> fun.apply(build(upstreamData, upstreamDataSize, ctx), ctx);
    }
}
