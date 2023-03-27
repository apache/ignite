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

package org.apache.ignite.internal.processors.cache.ttl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertCachesReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertDataStreamerReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;

/**
 * Checks that enabled read-only mode doesn't affect data expiration.
 */
public class CacheTtlReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Expiration timeout in seconds. */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** Cache configurations. */
    private static final CacheConfiguration[] CACHE_CONFIGURATIONS = getCacheConfigurations();

    /** Cache names. */
    private static final Collection<String> CACHE_NAMES =
        Stream.of(CACHE_CONFIGURATIONS).map(CacheConfiguration::getName).collect(Collectors.toList());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(CACHE_CONFIGURATIONS);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testTtlExpirationWorksInReadOnlyMode() throws Exception {
        Ignite grid = startGrid();

        assertEquals(ACTIVE, grid.cluster().state());

        assertCachesReadOnlyMode(grid.cluster().state() == ACTIVE_READ_ONLY, CACHE_NAMES);

        for (String cacheName : CACHE_NAMES) {
            assertEquals(cacheName, 0, grid.cache(cacheName).size());

            for (int i = 0; i < 10; i++)
                grid.cache(cacheName).put(i, i);

            assertEquals(cacheName, 10, grid.cache(cacheName).size());
        }

        grid.cluster().state(ACTIVE_READ_ONLY);
        assertEquals(ACTIVE_READ_ONLY, grid.cluster().state());

        assertCachesReadOnlyMode(grid.cluster().state() == ACTIVE_READ_ONLY, CACHE_NAMES);
        assertDataStreamerReadOnlyMode(grid.cluster().state() == ACTIVE_READ_ONLY, CACHE_NAMES);

        SECONDS.sleep(EXPIRATION_TIMEOUT + 1);

        for (String cacheName : CACHE_NAMES)
            assertEquals(cacheName, 0, grid.cache(cacheName).size());
    }

    /** */
    private static CacheConfiguration[] getCacheConfigurations() {
        CacheConfiguration[] cfgs = cacheConfigurations();

        List<CacheConfiguration> newCfgs = new ArrayList<>(cfgs.length);

        for (CacheConfiguration cfg : cfgs) {
            if (cfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT) {
                // Expiry policy cannot be used with TRANSACTIONAL_SNAPSHOT.
                continue;
            }

            cfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(SECONDS, EXPIRATION_TIMEOUT)));
            cfg.setEagerTtl(true);

            newCfgs.add(cfg);
        }

        return newCfgs.toArray(new CacheConfiguration[0]);
    }
}
