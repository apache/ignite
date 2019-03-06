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
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDataset;
import org.apache.ignite.ml.dataset.impl.local.LocalDataset;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriConsumer;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/**
 * A dataset providing an API that allows to perform generic computations on a distributed data represented as a set of
 * partitions distributed across a cluster or placed locally. Every partition contains a {@code context} (reliably
 * stored segment) and {@code data} (unreliably stored segment, which can be recovered from an upstream data and a
 * {@code context} if needed). Computations are performed in a {@code MapReduce} manner, what allows to reduce a
 * network traffic for most of the machine learning algorithms.
 *
 * <p>Dataset functionality allows to implement iterative machine learning algorithms via introducing computation
 * context. In case iterative algorithm requires to maintain a state available and updatable on every iteration this
 * state can be stored in the {@code context} of the partition and after that it will be available in further
 * computations even if the Ignite Cache partition will be moved to another node because of node failure or rebalancing.
 *
 * <p>Partition {@code context} should be {@link Serializable} to be saved in Ignite Cache. Partition {@code data}
 * should be {@link AutoCloseable} to allow system to clean up correspondent resources when partition {@code data} is
 * not needed anymore.
 *
 * @param <C> Type of a partition {@code context}.
 * @param <D> Type of a partition {@code data}.
 *
 * @see CacheBasedDataset
 * @see LocalDataset
 * @see DatasetFactory
 */
public interface Dataset<C extends Serializable, D extends AutoCloseable> extends AutoCloseable {
    /**
     * Applies the specified {@code map} function to every partition {@code data}, {@code context} and partition
     * index in the dataset and then reduces {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data}, {@code context} and {@link LearningEnvironment}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param identity Identity.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public <R> R computeWithCtx(IgniteTriFunction<C, D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce, R identity);

    /**
     * Applies the specified {@code map} function to every partition {@code data} and {@link LearningEnvironment}
     * in the dataset and then reduces {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data} and {@link LearningEnvironment}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param identity Identity.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public <R> R compute(IgniteBiFunction<D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce, R identity);

    /**
     * Applies the specified {@code map} function to every partition {@code data}, {@code context} and
     * {@link LearningEnvironment} in the dataset and then reduces {@code map} results to final
     * result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data}, {@code context} and {@link LearningEnvironment}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public default <R> R computeWithCtx(IgniteTriFunction<C, D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce) {
        return computeWithCtx(map, reduce, null);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} and {@link LearningEnvironment}
     * in the dataset and then reduces {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data} and {@link LearningEnvironment}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public default <R> R compute(IgniteBiFunction<D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce) {
        return compute(map, reduce, null);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} and {@code context} in the dataset
     * and then reduces {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data} and {@code context}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param identity Identity.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public default <R> R computeWithCtx(IgniteBiFunction<C, D, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        return computeWithCtx((ctx, data, env) -> map.apply(ctx, data), reduce, identity);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} in the dataset and then reduces
     * {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param identity Identity.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public default <R> R compute(IgniteFunction<D, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        return compute((data, env) -> map.apply(data), reduce, identity);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} and {@code context} in the dataset
     * and then reduces {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data} and {@code context}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public default <R> R computeWithCtx(IgniteBiFunction<C, D, R> map, IgniteBinaryOperator<R> reduce) {
        return computeWithCtx((ctx, data, env) -> map.apply(ctx, data), reduce);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} in the dataset and then reduces
     * {@code map} results to final result by using the {@code reduce} function.
     *
     * @param map Function applied to every partition {@code data}.
     * @param reduce Function applied to results of {@code map} to get final result.
     * @param <R> Type of a result.
     * @return Final result.
     */
    public default <R> R compute(IgniteFunction<D, R> map, IgniteBinaryOperator<R> reduce) {
        return compute((data, env) -> map.apply(data), reduce);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data}, {@code context} and
     * {@link LearningEnvironment} in the dataset.
     *
     * @param map Function applied to every partition {@code data}, {@code context} and partition index.
     */
    public default void computeWithCtx(IgniteTriConsumer<C, D, LearningEnvironment> map) {
        computeWithCtx((ctx, data, env) -> {
            map.accept(ctx, data, env);
            return null;
        }, (a, b) -> null);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} in the dataset and
     * {@link LearningEnvironment}.
     *
     * @param map Function applied to every partition {@code data} and partition index.
     */
    public default void compute(IgniteBiConsumer<D, LearningEnvironment> map) {
        compute((data, env) -> {
            map.accept(data, env);
            return null;
        }, (a, b) -> null);
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} and {@code context} in the dataset.
     *
     * @param map Function applied to every partition {@code data} and {@code context}.
     */
    public default void computeWithCtx(IgniteBiConsumer<C, D> map) {
        computeWithCtx((ctx, data, env) -> map.accept(ctx, data));
    }

    /**
     * Applies the specified {@code map} function to every partition {@code data} in the dataset.
     *
     * @param map Function applied to every partition {@code data}.
     */
    public default void compute(IgniteConsumer<D> map) {
        compute((data, env) -> map.accept(data));
    }

    /**
     * Wraps this dataset into the specified wrapper to introduce new functionality based on {@code compute} and
     * {@code computeWithCtx} methods.
     *
     * @param wrapper Dataset wrapper.
     * @param <I> Type of a new wrapped dataset.
     * @return New wrapped dataset.
     */
    public default <I extends Dataset<C ,D>> I wrap(IgniteFunction<Dataset<C, D>, I> wrapper) {
        return wrapper.apply(this);
    }
}
