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

package org.apache.ignite.scalar.lang

import org.apache.ignite.internal.util.lang.GridInClosure3

/**
 * Peer deploy aware adapter for Java's `GridInClosure3`.
 */
class ScalarInClosure3[T1, T2, T3](private val f: (T1, T2, T3) => Unit) extends GridInClosure3[T1, T2, T3] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    def apply(t1: T1, t2: T2, t3: T3) {
        f(t1, t2, t3)
    }
}
