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
import org.apache.ignite.ml.dataset.api.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Builder that accepts a partition {@code upstream} data and makes partition {@code context}. This builder is used to
 * build a partition {@code context} and assumed to be called only once for every partition during a dataset
 * initialization.
 *
 * @param <K> type of a key in {@code upstream} data
 * @param <V> type of a value in {@code upstream} data
 * @param <C> type of a partition {@code context}
 *
 * @see EmptyContextBuilder
 */
@FunctionalInterface
public interface PartitionContextBuilder<K, V, C extends Serializable> extends Serializable {
    /**
     * Builds a new partition {@code context} from an {@code upstream} data.
     *
     * @param upstreamData partition {@code upstream} data
     * @param upstreamDataSize partition {@code upstream} data size
     * @return partition {@code context}
     */
    public C build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize);

    /**
     * Makes a composed partition {@code context} builder that first builds a {@code context} and then applies the
     * specified function on the result.
     *
     * @param fun function that applied after first partition {@code context} is built
     * @param <C2> new type of a partition {@code context}
     * @return composed partition {@code context} builder
     */
    default public <C2 extends Serializable> PartitionContextBuilder<K, V, C2> andThen(IgniteFunction<C, C2> fun) {
        return (upstreamData, upstreamDataSize) -> fun.apply(build(upstreamData, upstreamDataSize));
    }
}
