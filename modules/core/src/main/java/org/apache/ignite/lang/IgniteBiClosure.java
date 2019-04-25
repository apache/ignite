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

package org.apache.ignite.lang;

import java.io.Serializable;

/**
 * Defines generic closure with two parameters. Bi-Closure is a simple executable which
 * accepts two parameters and returns a value.
 *
 * @param <E1> Type of the first parameter.
 * @param <E2> Type of the second parameter.
 * @param <R> Type of the closure's return value.
 */
public interface IgniteBiClosure<E1, E2, R> extends Serializable {
    /**
     * Closure body.
     *
     * @param e1 First parameter.
     * @param e2 Second parameter.
     * @return Closure return value.
     */
    public abstract R apply(E1 e1, E2 e2);
}