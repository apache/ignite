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

import org.apache.ignite.IgniteCheckedException;

/**
 * Closure taking 2 arguments.
 */
public interface GridPlainClosure2<T1, T2, R> {
    /**
     * @param arg1 Closure argument.
     * @param arg2 Closure argument.
     * @return Closure execution result.
     * @throws IgniteCheckedException If error occurred.
     */
    public R apply(T1 arg1, T2 arg2) throws IgniteCheckedException;
}