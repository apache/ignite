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

import org.apache.ignite.lang.IgniteBiInClosure

/**
 * Peer deploy aware adapter for Java's `GridInClosure2`.
 */
class ScalarInClosure2[T1, T2](private val f: (T1, T2) => Unit) extends IgniteBiInClosure[T1, T2] {
    assert(f != null)

    /**
     * Delegates to passed in function.
     */
    def apply(t1: T1, t2: T2) {
        f(t1, t2)
    }
}
