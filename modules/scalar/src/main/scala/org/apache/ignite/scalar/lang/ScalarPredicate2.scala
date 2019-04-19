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

import org.apache.ignite.lang.IgniteBiPredicate

/**
 * Peer deploy aware adapter for Java's `GridPredicate2`.
 */
class ScalarPredicate2[T1, T2](private val p: (T1, T2) => Boolean) extends IgniteBiPredicate[T1, T2] {
    assert(p != null)

    /**
     * Delegates to passed in function.
     */
    def apply(e1: T1, e2: T2) = p(e1, e2)
}
