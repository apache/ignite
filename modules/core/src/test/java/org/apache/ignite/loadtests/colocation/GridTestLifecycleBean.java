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

package org.apache.ignite.loadtests.colocation;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Lifecycle bean.
 */
public class GridTestLifecycleBean implements LifecycleBean {
    /** */
    @IgniteInstanceResource
    private Ignite g;

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType type) {
        if (type == LifecycleEventType.AFTER_NODE_START) {
            IgniteCache<GridTestKey, Long> cache = g.cache("partitioned");

            assert cache != null;

            cache.loadCache(null, GridTestConstants.LOAD_THREADS, GridTestConstants.ENTRY_COUNT);
        }
    }
}