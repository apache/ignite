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

import org.apache.ignite.internal.util.lang.GridClosure3

/**
 * Peer deploy aware adapter for Java's `GridClosure3`.
 */
class ScalarClosure3[E1, E2, E3, R](private val f: (E1, E2, E3) => R) extends GridClosure3[E1, E2, E3, R] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    def apply(e1: E1, e2: E2, e3: E3): R = {
        f(e1, e2, e3)
    }
}
