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

/**
 * Defines generic closure with one parameter. Closure is a simple executable which accepts a parameter and
 * returns a value.
 * <p>
 * In Ignite closures are mainly used for executing distributed computations
 * on the grid, like in {@link org.apache.ignite.IgniteCompute#apply(IgniteClosure, Object)} method.
 *
 * @param <E> Type of closure parameter.
 * @param <R> Type of the closure return value.
 */
public interface IgniteClosure<E, R> extends Serializable {
    /**
     * Closure body.
     *
     * @param e Closure parameter.
     * @return Closure return value.
     */
    public R apply(E e);
}