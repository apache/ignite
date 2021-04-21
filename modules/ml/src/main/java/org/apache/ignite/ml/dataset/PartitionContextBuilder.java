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
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Builder that accepts a partition {@code upstream} data and makes partition {@code context}. This builder is used to
 * build a partition {@code context} and assumed to be called only once for every partition during a dataset
 * initialization.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 * @param <C> Type of a partition {@code context}.
 *
 * @see EmptyContextBuilder
 */
@FunctionalInterface
public interface PartitionContextBuilder<K, V, C extends Serializable> extends Serializable {
    /**
     * Builds a new partition {@code context} from an {@code upstream} data.
     * Important: there is no guarantee that there will be no more than one UpstreamEntry with given key,
     * UpstreamEntry should be thought rather as a container saving all data from upstream, but omitting uniqueness
     * constraint. This constraint is omitted to allow upstream data transformers in {@link DatasetBuilder} replicating
     * entries. For example it can be useful for bootstrapping.
     *
     * @param env Learning environment.
     * @param upstreamData Partition {@code upstream} data.
     * @param upstreamDataSize Partition {@code upstream} data size.
     * @return Partition {@code context}.
     */
    public C build(LearningEnvironment env, Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize);

    /**
     * Builds a new partition {@code context} from an {@code upstream} data.
     * Important: there is no guarantee that there will be no more than one UpstreamEntry with given key,
     * UpstreamEntry should be thought rather as a container saving all data from upstream, but omitting uniqueness
     * constraint. This constraint is omitted to allow upstream data transformers in {@link DatasetBuilder} replicating
     * entries. For example it can be useful for bootstrapping.
     *
     * @param env Learning environment.
     * @param upstreamData Partition {@code upstream} data.
     * @param upstreamDataSize Partition {@code upstream} data size.
     * @return Partition {@code context}.
     */
    public default C build(LearningEnvironment env, Stream<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize) {
        return build(env, upstreamData.iterator(), upstreamDataSize);
    }

    /**
     * Makes a composed partition {@code context} builder that first builds a {@code context} and then applies the
     * specified function on the result.
     *
     * @param fun Function that applied after first partition {@code context} is built.
     * @param <C2> New type of a partition {@code context}.
     * @return Composed partition {@code context} builder.
     */
    public default <C2 extends Serializable> PartitionContextBuilder<K, V, C2> andThen(IgniteFunction<C, C2> fun) {
        return (env, upstreamData, upstreamDataSize) -> fun.apply(build(env, upstreamData, upstreamDataSize));
    }
}
