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

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;

/**
 * Represents an operation that accepts a two input arguments and returns no result. Unlike most other functional
 * interfaces, {@code IgniteThrowableBiConsumer} is expected to operate via side-effects.
 *
 * Also it is able to throw {@link IgniteCheckedException} unlike {@link java.util.function.BiConsumer}.
 *
 * @param <T> the type of the first argument to the predicate
 * @param <U> the type of the second argument the predicate
 */
@FunctionalInterface
public interface IgniteThrowableBiConsumer<T, U> extends Serializable {
    /**
     * Performs this operation on the given arguments.
     *
     * @param t The first input argument.
     * @param u The second input argument.
     * @throws IgniteCheckedException If body execution was failed.
     */
    public void accept(T t, U u) throws IgniteCheckedException;
}
