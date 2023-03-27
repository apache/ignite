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

package org.apache.ignite.ml;

import java.io.Serializable;
import java.util.function.BiFunction;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/** Basic interface for all models. */
@FunctionalInterface
public interface IgniteModel<T, V> extends Model<T, V>, Serializable {
    /**
     * Combines this model with other model via specified combiner
     *
     * @param other Other model.
     * @param combiner Combiner.
     * @return Combination of models.
     */
    public default <X, W> IgniteModel<T, X> combine(IgniteModel<T, W> other, BiFunction<V, W, X> combiner) {
        return v -> combiner.apply(predict(v), other.predict(v));
    }

    /**
     * Get a composition model of the form {@code x -> after(mdl(x))}.
     *
     * @param after Model to apply after this model.
     * @param <V1> Type of output of function applied after this model.
     * @return Composition model of the form {@code x -> after(mdl(x))}.
     */
    public default <V1> IgniteModel<T, V1> andThen(IgniteModel<V, V1> after) {
        IgniteModel<T, V> self = this;
        return new IgniteModel<T, V1>() {
            /** {@inheritDoc} */
            @Override public V1 predict(T input) {
                return after.predict(self.predict(input));
            }

            /** {@inheritDoc} */
            @Override public void close() {
                self.close();
                after.close();
            }
        };
    }

    /**
     * Get a composition model of the form {@code x -> after(mdl(x))}.
     *
     * @param after Function to apply after this model.
     * @param <V1> Type of input of function applied before this model.
     * @return Composition model of the form {@code x -> after(mdl(x))}.
     */
    public default <V1> IgniteModel<T, V1> andThen(IgniteFunction<V, V1> after) {
        IgniteModel<T, V> self = this;
        return new IgniteModel<T, V1>() {
            /** {@inheritDoc} */
            @Override public V1 predict(T input) {
                return after.apply(self.predict(input));
            }

            /** {@inheritDoc} */
            @Override public void close() {
                self.close();
            }
        };
    }

    /**
     * Get a composition model of the form {@code x -> mdl(before(x))}.
     *
     * @param before Function to apply before this model.
     * @param <V1> Type of input of function applied before this model.
     * @return Composition model of the form {@code x -> after(mdl(x))}.
     */
    public default <V1> IgniteModel<V1, V> andBefore(IgniteFunction<V1, T> before) {
        IgniteModel<T, V> self = this;
        return new IgniteModel<V1, V>() {
            /** {@inheritDoc} */
            @Override public V predict(V1 input) {
                return self.predict(before.apply(input));
            }

            /** {@inheritDoc} */
            @Override public void close() {
                self.close();
            }
        };
    }

    /**
     * @param pretty Use pretty mode.
     */
    public default String toString(boolean pretty) {
        return getClass().getSimpleName();
    }

    /** {@inheritDoc} */
    @Override public default void close() {
        // Do nothing.
    }
}
