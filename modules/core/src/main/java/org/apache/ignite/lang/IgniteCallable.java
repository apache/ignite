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
import java.util.concurrent.Callable;

/**
 * Grid-aware adapter for {@link Callable} implementations. It adds {@link Serializable} interface
 * to {@link Callable} object. Use this class for executing distributed computations on the grid,
 * like in {@link org.apache.ignite.IgniteCompute#call(IgniteCallable)} method.
 */
public interface IgniteCallable<V> extends Callable<V>, Serializable {
}