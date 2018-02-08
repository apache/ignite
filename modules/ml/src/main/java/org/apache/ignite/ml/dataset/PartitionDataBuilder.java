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
