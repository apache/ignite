/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml;

import java.util.function.BiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/** Basic interface for all models. */
public interface Model<T, V> extends IgniteFunction<T, V> {
    /**
     * Combines this model with other model via specified combiner
     *
     * @param other Other model.
     * @param combiner Combiner.
     * @return Combination of models.
     */
    public default <X, W> Model<T, X> combine(Model<T, W> other, BiFunction<V, W, X> combiner) {
        return v -> combiner.apply(apply(v), other.apply(v));
    }

    /**
     * @param pretty Use pretty mode.
     */
    default public String toString(boolean pretty) {
        return getClass().getSimpleName();
    }
}
