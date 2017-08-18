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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractMetricsSelfTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Local cache metrics test in ATOMIC mode without readthrough.
 */
public class GridCacheLocalAtomicMetricsNoReadThroughSelfTest extends GridCacheAbstractMetricsSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setCacheMode(LOCAL);
        cfg.setReadThrough(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }
}
