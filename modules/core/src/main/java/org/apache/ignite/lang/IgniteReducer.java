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

package org.apache.ignite.lang;

import java.io.Serializable;
import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Defines generic reducer that collects multiple values and reduces them into one.
 * Reducers are useful in computations when results from multiple remote jobs need
 * to be reduced into one, e.g. {@link org.apache.ignite.IgniteCompute#call(Collection, IgniteReducer)} method.
 *
 * @param <E> Type of collected values.
 * @param <R> Type of reduced value.
 */
public interface IgniteReducer<E, R> extends Serializable {
    /**
     * Collects given value. If this method returns {@code false} then {@link #reduce()}
     * will be called right away. Otherwise caller will continue collecting until all
     * values are processed.
     *
     * @param e Value to collect.
     * @return {@code true} to continue collecting, {@code false} to instruct caller to stop
     *      collecting and call {@link #reduce()} method.
     */
    public boolean collect(@Nullable E e);

    /**
     * Reduces collected values into one.
     *
     * @return Reduced value.
     */
    public R reduce();
}