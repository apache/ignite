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

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;

/**
 * Represents a function that accepts one argument and produces a result. Unlike {@link java.util.function.Function}
 * it is able to throw {@link IgniteCheckedException}.
 *
 * @param <E> The type of the input to the function.
 * @param <R> The type of the result of the function.
 */
public interface IgniteThrowableFunction<E, R> extends Serializable {
    /**
     * Applies this function to the given argument.
     *
     * @param e The function argument.
     * @return The function result.
     * @throws IgniteCheckedException if body execution was failed.
     */
    public R apply(E e) throws IgniteCheckedException;
}
