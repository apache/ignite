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

package org.apache.ignite.internal.util.function;

import java.util.Objects;
import java.util.function.Function;

/**
 * Specific interface for transmitting exceptions from lambda to external method without a catch.
 *
 * @param <T> The type of the input to the function.
 * @param <R> The type of the result of the function.
 * @param <E> The exception to be thrown from the body of the function.
 */
@FunctionalInterface
public interface ThrowableFunction<R, T, E extends Exception> {
    /**
     * Applies this function to the given argument.
     *
     * @param t The function argument.
     * @return The function result.
     * @throws E If failed.
     */
    R apply(T t) throws E;

    /**
     * Returns a composed function that first applies this function to its input, and then applies the after function to the result.
     *
     * @param <V> The type of output of the {@code after} function, and of the composed function.
     * @param after The function to apply after this function is applied.
     * @return A composed function that first applies this function and then applies the {@code after} function.
     * @see java.util.function.Function#andThen(Function)
     */
    default <V> ThrowableFunction<V, T, E> andThen(ThrowableFunction<V, R, E> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }

    /**
     * Returns a function that always returns its input argument.
     *
     * @param <I> The type of the input and output objects to the function.
     * @return A function that always returns its input argument.
     * @see Function#identity()
     */
    static <I, E extends Exception> ThrowableFunction<I, I, E> identity() {
        return (I t) -> t;
    }
}
