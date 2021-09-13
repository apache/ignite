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
}
