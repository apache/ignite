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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxPreloadAbstractTest;
import org.apache.ignite.testframework.MvccFeatureChecker;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests cache transaction during preloading.
 */
public class GridCacheNearTxPreloadSelfTest extends IgniteTxPreloadAbstractTest {
    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setBackups(4);

        return cfg;
    }
}
