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
import java.util.function.Consumer;

/**
 * Serializable tri-consumer.
 *
 * @param <A> First parameter type.
 * @param <B> Second parameter type.
 * @param <C> Third parameter type.
 */
@FunctionalInterface
public interface IgniteTriConsumer<A, B, C> extends Serializable {
    /**
     * Analogous to 'accept' in {@link Consumer} version, but with three parameters.
     *
     * @param first First parameter.
     * @param second Second parameter.
     * @param third Third parameter.
     */
    public void accept(A first, B second, C third);
}
