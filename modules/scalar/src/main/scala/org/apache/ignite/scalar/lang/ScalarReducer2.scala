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

import org.apache.ignite.internal.util.lang.IgniteReducer2

import scala.collection._

/**
 * Peer deploy aware adapter for Java's `GridReducer2`.
 */
class ScalarReducer2[E1, E2, R](private val r: (Seq[E1], Seq[E2]) => R) extends IgniteReducer2[E1, E2, R] {
    assert(r != null)

    private val buf1 = new mutable.ListBuffer[E1]
    private val buf2 = new mutable.ListBuffer[E2]

    /**
     * Delegates to passed in function.
     */
    def apply = r(buf1.toSeq, buf2.toSeq)

    /**
     * Collects given values.
     *
     * @param e1 Value to collect for later reduction.
     * @param e2 Value to collect for later reduction.
     */
    def collect(e1: E1, e2: E2) = {
        buf1 += e1
        buf2 += e2

        true
    }
}
