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

package org.apache.ignite.yardstick.thin.cache;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.yardstick.IgniteThinAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 * Abstract class for thin client benchmarks which use cache.
 */
public abstract class IgniteThinCacheAbstractBenchmark<K, V> extends IgniteThinAbstractBenchmark {
    /** Cache. */
    protected ClientCache<K, V> cache;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cache = cache();
    }

    /**
     * Each benchmark must determine which cache will be used.
     *
     * @return ClientCache Cache to use.
     */
    protected abstract ClientCache<K, V> cache();
}
