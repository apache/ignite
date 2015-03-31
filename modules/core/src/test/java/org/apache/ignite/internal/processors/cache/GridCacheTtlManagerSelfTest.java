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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.expiry.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * TTL manager self test.
 */
public class GridCacheTtlManagerSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test cache mode. */
    protected CacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTtl() throws Exception {
        checkTtl(LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTtl() throws Exception {
        checkTtl(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTtl() throws Exception {
        checkTtl(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void checkTtl(CacheMode mode) throws Exception {
        cacheMode = mode;

        final IgniteKernal g = (IgniteKernal)startGrid(0);

        try {
            final String key = "key";

            g.cache(null).withExpiryPolicy(
                    new TouchedExpiryPolicy(new Duration(MILLISECONDS, 1000))).put(key, 1);

            assertEquals(1, g.cache(null).get(key));

            U.sleep(1100);

            GridTestUtils.retryAssert(log, 10, 100, new CAX() {
                @Override public void applyx() {
                    // Check that no more entries left in the map.
                    assertNull(g.cache(null).get(key));

                    if (!g.internalCache().context().deferredDelete())
                        assertNull(g.internalCache().map().getEntry(key));
                }
            });
        }
        finally {
            stopAllGrids();
        }
    }
}
