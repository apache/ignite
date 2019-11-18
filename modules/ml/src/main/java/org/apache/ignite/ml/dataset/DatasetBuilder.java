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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
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
     * @param localLearningEnv Local learning environment.
     * @return Dataset.
     */
    public <C extends Serializable, D extends AutoCloseable> Dataset<C, D> build(
        LearningEnvironmentBuilder envBuilder,
        PartitionContextBuilder<K, V, C> partCtxBuilder,
        PartitionDataBuilder<K, V, C, D> partDataBuilder,
        LearningEnvironment localLearningEnv);

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
