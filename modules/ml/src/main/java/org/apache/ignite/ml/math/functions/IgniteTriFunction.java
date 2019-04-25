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

package org.apache.ignite.ml.math.functions;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/** Serializable TriFunction (A, B, C) -> R. */
@FunctionalInterface
public interface IgniteTriFunction<A, B, C, R> extends Serializable {
    /** */
    public R apply(A a, B b, C c);

    /** */
    public default <V> IgniteTriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);

        return (A a, B b, C c) -> after.apply(apply(a, b, c));
    }
}
