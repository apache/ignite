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

import org.apache.ignite.internal.util.lang.GridPeerDeployAwareAdapter
import org.apache.ignite.lang.{IgniteCallable, IgniteOutClosure}

import java.util.concurrent.Callable

/**
 * Peer deploy aware adapter for Java's `GridOutClosure`.
 */
class ScalarOutClosure[R](private val f: () => R) extends GridPeerDeployAwareAdapter
    with IgniteOutClosure[R] with IgniteCallable[R] {
    assert(f != null)

    peerDeployLike(f)

    /**
     * Delegates to passed in function.
     */
    def apply: R = {
        f()
    }

    /**
     * Delegates to passed in function.
     */
    def call: R = {
        f()
    }
}
