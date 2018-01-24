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

package org.apache.ignite.ml.dlc;

import java.io.Serializable;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Distributed Learning Context provides the API which allows to perform iterative computation tasks on a distributed
 * datasets. Every computation performed via Distributed Learning Context works with {@link DLCPartition} which consists
 * of replicated data and recoverable data. Computation task can modify these segments to maintain the iterative
 * algorithm context.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
public interface DLC<K, V, Q extends Serializable, W extends AutoCloseable> extends AutoCloseable {
    /**
     * Computes a given function on every DLC partition in current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code mapper} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     * @param reducer reducer of the results
     * @param identity identity value
     * @param <R> result type
     * @return final reduced result
     */
    public <R> R compute(IgniteBiFunction<DLCPartition<K, V, Q, W>, Integer, R> mapper, IgniteBinaryOperator<R> reducer,
        R identity);

    /**
     * Computes a given function on every DLC partition in current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code mapper} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     * @param reducer reducer of the results
     * @param <R> result type
     * @return final reduced result
     */
    default public <R> R compute(IgniteBiFunction<DLCPartition<K, V, Q, W>, Integer, R> mapper,
        IgniteBinaryOperator<R> reducer) {
        return compute(mapper, reducer, null);
    }

    /**
     * Computes a given function on every DLC partition in current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code mapper} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     * @param reducer reducer of the results
     * @param identity identity value
     * @param <R> result type
     * @return final reduced result
     */
    default public <R> R compute(IgniteFunction<DLCPartition<K, V, Q, W>, R> mapper, IgniteBinaryOperator<R> reducer,
        R identity) {
        return compute((part, partIdx) -> mapper.apply(part), reducer, identity);
    }

    /**
     * Computes a given function on every DLC partition in current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code mapper} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     * @param reducer reducer of the results
     * @param <R> result type
     * @return final reduced result
     */
    default public <R> R compute(IgniteFunction<DLCPartition<K, V, Q, W>, R> mapper, IgniteBinaryOperator<R> reducer) {
        return compute((part, partIdx) -> mapper.apply(part), reducer);
    }

    /**
     * Computes a given function on every DLC partition in current learning context independently. The goal of this
     * approach is to perform {@code mapper} locally on the nodes where partitions are placed and do not involve network
     * subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     */
    default public void compute(IgniteBiConsumer<DLCPartition<K, V, Q, W>, Integer> mapper) {
        compute((part, partIdx) -> {
            mapper.accept(part, partIdx);
            return null;
        }, (a, b) -> null);
    }

    /**
     * Computes a given function on every DLC partition in current learning context independently. The goal of this
     * approach is to perform {@code mapper} locally on the nodes where partitions are placed and do not involve network
     * subsystem where it's possible.
     *
     * @param mapper mapper function applied on every partition
     */
    default public void compute(IgniteConsumer<DLCPartition<K, V, Q, W>> mapper) {
        compute((part, partIdx) -> mapper.accept(part));
    }
}
