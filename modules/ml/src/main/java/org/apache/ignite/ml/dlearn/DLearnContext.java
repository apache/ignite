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

package org.apache.ignite.ml.dlearn;

import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Learning context is a context maintained during a whole learning process. The context provides an ability to perform
 * calculations in map-reduce manner and guarantees maintenance of the partition states between compute calls.
 *
 * @param <P> type of learning context partition
 */
public interface DLearnContext<P> {
    /**
     * Computes a given function on every d-learn partition in current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code mapper} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     * @param reducer  reducer of the results
     * @param <R> result type
     * @return final reduced result
     */
    public <R> R compute(IgniteBiFunction<P, Integer, R> mapper, IgniteBinaryOperator<R> reducer);

    /**
     * Computes a given function on every d-learn partition in current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code mapper} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     * @param reducer  reducer of the results
     * @param <R> result type
     * @return final reduced result
     */
    default public <R> R compute(IgniteFunction<P, R> mapper, IgniteBinaryOperator<R> reducer) {
        return compute((part, partIdx) -> mapper.apply(part), reducer);
    }

    /**
     * Computes a given function on every d-learn partition in current learning context independently. The goal of this
     * approach is to perform {@code mapper} locally on the nodes where partitions are placed and do not involve network
     * subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     */
    public void compute(IgniteBiConsumer<P, Integer> mapper);

    /**
     * Computes a given function on every d-learn partition in current learning context independently. The goal of this
     * approach is to perform {@code mapper} locally on the nodes where partitions are placed and do not involve network
     * subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     */
    default public void compute(IgniteConsumer<P> mapper) {
        compute((part, partIdx) -> mapper.accept(part));
    }

    /**
     * Transforms current learning context into another learning context which contains another type of d-learn
     * partitions. Transformation doesn't involve new cache instantiation or network data transfer, it just performs
     * {@code #transform(IgniteBiConsumer, DLearnPartitionFactory)} locally on every partition in the current context
     * and saves results into the same context cache, but with a new context id.
     *
     * @param transformer transformer
     * @param <T> type of new d-learn partition
     * @return new learning context
     */
    public <T, C extends DLearnContext<T>> C transform(DLearnContextTransformer<P, T, C> transformer);
}
