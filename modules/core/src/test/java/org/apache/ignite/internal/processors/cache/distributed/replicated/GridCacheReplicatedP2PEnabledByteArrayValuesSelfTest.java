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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

/**
 * Tests for byte array values in REPLICATED caches with P2P enabled.
 */
public class GridCacheReplicatedP2PEnabledByteArrayValuesSelfTest extends
    GridCacheAbstractReplicatedByteArrayValuesSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean peerClassLoading() {
        return true;
    }
}