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

package org.apache.ignite.scalar.lang

import org.apache.ignite._
import org.apache.ignite.internal.util.lang.GridClosure3X

/**
 * Peer deploy aware adapter for Java's `GridClosure3X`.
 */
class ScalarClosure3X[E1, E2, E3, R](private val f: (E1, E2, E3) => R) extends GridClosure3X[E1, E2, E3, R] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[IgniteCheckedException])
    def applyx(e1: E1, e2: E2, e3: E3): R = {
        f(e1, e2, e3)
    }
}
