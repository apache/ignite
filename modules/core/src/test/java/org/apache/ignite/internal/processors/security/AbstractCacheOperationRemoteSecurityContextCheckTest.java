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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;

/**
 *
 */
public abstract class AbstractCacheOperationRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
        };
    }

    /**
     * Getting the key that is contained on primary partition on passed node for cache.
     *
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return Key.
     */
    protected Integer primaryKey(IgniteEx ignite, String cacheName) {
        return findKeys(ignite.localNode(), ignite.cache(cacheName), 1, 0, 0)
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(ignite.name() + " isn't primary node for any key."));
    }

    /**
     * Getting the key that is contained on primary partition on passed node for {@link #CACHE_NAME} cache.
     *
     * @param ignite Node.
     * @return Key.
     */
    protected Integer primaryKey(IgniteEx ignite) {
        return primaryKey(ignite, CACHE_NAME);
    }

    /**
     * Getting the key that is contained on primary partition on passed node for {@link #CACHE_NAME} cache.
     *
     * @param nodeName Node name.
     * @return Key.
     */
    protected Integer primaryKey(String nodeName) {
        return primaryKey((IgniteEx)G.ignite(nodeName));
    }
}
