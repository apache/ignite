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
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.trainers.transformers.BaggingUpstreamTransformer;

/**
 * A builder constructing instances of a {@link Dataset}. Implementations of this interface encapsulate logic of
 * building specific datasets such as allocation required data structures and initialization of {@code context} part of
 * partitions.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 *
 * @see CacheBasedDatasetBuilder
 * @see LocalDatasetBuilder
 * @see Dataset
 */
public interface DatasetBuilder<K, V> {
    /**
     * Constructs a new instance of {@link Dataset} that includes allocation required data structures and
     * initialization of {@code context} part of partitions.
     *
     * @param envBuilder Learning environment builder.
     * @param partCtxBuilder Partition {@code context} builder.
     * @param partDataBuilder Partition {@code data} builder.
     * @param <C> Type of a partition {@code context}.
     * @param <D> Type of a partition {@code data}.
     * @return Dataset.
     */
    public <C extends Serializable, D extends AutoCloseable> Dataset<C, D> build(
        LearningEnvironmentBuilder envBuilder,
        PartitionContextBuilder<K, V, C> partCtxBuilder,
        PartitionDataBuilder<K, V, C, D> partDataBuilder);

    /**
     * Returns new instance of {@link DatasetBuilder} with new {@link UpstreamTransformerBuilder} added
     * to chain of upstream transformer builders. When needed, each builder in chain first transformed into
     * {@link UpstreamTransformer}, those are in turn composed together one after another forming
     * final {@link UpstreamTransformer}.
     * This transformer is applied to upstream data before it is passed
     * to {@link PartitionDataBuilder} and {@link PartitionContextBuilder}. This is needed to allow
     * transformation to upstream data which are agnostic of any changes that happen after.
     * Such transformations may be used for deriving meta-algorithms such as bagging
     * (see {@link BaggingUpstreamTransformer}).
     *
     * @return Returns new instance of {@link DatasetBuilder} with new {@link UpstreamTransformerBuilder} added
     * to chain of upstream transformer builders.
     */
    public DatasetBuilder<K, V> withUpstreamTransformer(UpstreamTransformerBuilder builder);

    /**
     * Returns new instance of DatasetBuilder using conjunction of internal filter and {@code filterToAdd}.
     * @param filterToAdd Additional filter.
     */
    public DatasetBuilder<K,V> withFilter(IgniteBiPredicate<K,V> filterToAdd);
}
