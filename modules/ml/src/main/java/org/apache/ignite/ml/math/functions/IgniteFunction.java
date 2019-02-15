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

package org.apache.ignite.ml.math.functions;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Serializable function.
 *
 * @see java.util.function.Function
 */
public interface IgniteFunction<T, R> extends Function<T, R>, Serializable {
    /**
     * {@link IgniteFunction} returning specified constant.
     *
     * @param r Constant to return.
     * @param <T> Type of input.
     * @param <R> Type of output.
     * @return {@link IgniteFunction} returning specified constant.
     */
    // TODO: IGNITE-10653 Maybe we should add toString description to identity and constant.
    public static <T, R> IgniteFunction<T, R> constant(R r) {
        return (IgniteFunction<T, R>)t -> r;
    }

    /**
     * Compose this function and given function.
     *
     * @param after Function to compose with.
     * @param <V> Type of value which result of {@code after} extends.
     * @return Functions composition.
     */
    default <V> IgniteFunction<T, V> andThen(IgniteFunction<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }

    /**
     * Identity function.
     *
     * @param <T> Type of input and output.
     * @return Identity function.
     */
    public static <T> IgniteFunction<T, T> identity() {
        return (IgniteFunction<T, T>)t -> t;
    }
}
