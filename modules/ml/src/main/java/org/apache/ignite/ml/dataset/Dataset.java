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
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriConsumer;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/**
 * Distributed Learning Context provides the API which allows to perform iterative computation tasks on a distributed
 * datasets. Every computation performed via Distributed Learning Context works with {@link DLCPartition} which consists
 * of replicated data and recoverable data. Computation task can modify these segments to maintain the iterative
 * algorithm context.
 *
 * @param <C>
 * @param <D>
 */
public interface Dataset<C extends Serializable, D extends AutoCloseable> extends AutoCloseable {
    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param identity
     * @param <R>
     * @return
     */
    public <R> R computeWithCtx(IgniteTriFunction<C, D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity);

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param identity
     * @param <R>
     * @return
     */
    public <R> R compute(IgniteBiFunction<D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity);

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param <R>
     * @return
     */
    default public <R> R computeWithCtx(IgniteTriFunction<C, D, Integer, R> map, IgniteBinaryOperator<R> reduce) {
        return computeWithCtx(map, reduce, null);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param <R>
     * @return
     */
    default public <R> R compute(IgniteBiFunction<D, Integer, R> map, IgniteBinaryOperator<R> reduce) {
        return compute(map, reduce, null);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param identity
     * @param <R>
     * @return
     */
    default public <R> R computeWithCtx(IgniteBiFunction<C, D, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        return computeWithCtx((ctx, data, partIdx) -> map.apply(ctx, data), reduce, identity);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param identity
     * @param <R>
     * @return
     */
    default public <R> R compute(IgniteFunction<D, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        return compute((data, partIdx) -> map.apply(data), reduce, identity);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param <R>
     * @return
     */
    default public <R> R computeWithCtx(IgniteBiFunction<C, D, R> map, IgniteBinaryOperator<R> reduce) {
        return computeWithCtx((ctx, data, partIdx) -> map.apply(ctx, data), reduce);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     * @param reduce
     * @param <R>
     * @return
     */
    default public <R> R compute(IgniteFunction<D, R> map, IgniteBinaryOperator<R> reduce) {
        return compute((data, partIdx) -> map.apply(data), reduce);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     */
    default public void computeWithCtx(IgniteTriConsumer<C, D, Integer> map) {
        computeWithCtx((ctx, data, partIdx) -> {
            map.accept(ctx, data, partIdx);
            return null;
        }, (a, b) -> null);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     */
    default public void compute(IgniteBiConsumer<D, Integer> map) {
        compute((data, partIdx) -> {
            map.accept(data, partIdx);
            return null;
        }, (a, b) -> null);
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     */
    default public void computeWithCtx(IgniteBiConsumer<C, D> map) {
        computeWithCtx((ctx, data, partIdx) -> map.accept(ctx, data));
    }

    /**
     * Computes the given function on every partition in the current learning context independently and then reduces
     * results into one final single result. The goal of this approach is to perform {@code map} locally on the nodes
     * where partitions are placed and do not involve network subsystem where it's possible.
     *
     * @param map
     */
    default public void compute(IgniteConsumer<D> map) {
        compute((data, partIdx) -> map.accept(data));
    }

    /**
     *
     * @param wrapper
     * @param <I>
     * @return
     */
    default public <I extends Dataset<C ,D>> I wrap(IgniteFunction<Dataset<C, D>, I> wrapper) {
        return wrapper.apply(this);
    }
}
