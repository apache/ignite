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

package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 * Continuous queries tests for replicated cache with disabled P2P class loading.
 */
public class GridCacheContinuousQueryReplicatedP2PDisabledSelfTest extends GridCacheContinuousQueryReplicatedSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean peerClassLoadingEnabled() {
        return false;
    }
}