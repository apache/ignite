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

package org.apache.ignite.internal.processors.cache.query;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * {@link ScanQueryConcurrentUpdatesAbstractTest} with caches created, updates and destroyed using Java API.
 */
public class ScanQueryConcurrentUpdatesTest extends ScanQueryConcurrentUpdatesAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Integer> createCache(String cacheName, CacheMode cacheMode,
                                                                  Duration expiration) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(cacheMode);
        if (expiration != null) {
            cacheCfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(expiration));
            cacheCfg.setEagerTtl(true);
        }

        return grid(0).createCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override protected void updateCache(IgniteCache<Integer, Integer> cache, int recordsNum) {
        for (int i = 0; i < recordsNum; i++)
            cache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void destroyCache(IgniteCache<Integer, Integer> cache) {
        cache.destroy();
    }
}
