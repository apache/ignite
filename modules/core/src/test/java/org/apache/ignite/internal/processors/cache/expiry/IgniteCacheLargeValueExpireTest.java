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

package org.apache.ignite.internal.processors.cache.expiry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheLargeValueExpireTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int PAGE_SIZE = 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        MemoryConfiguration dbCfg = new MemoryConfiguration();
        dbCfg.setPageSize(1024);

        cfg.setMemoryConfiguration(dbCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpire() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            checkExpire(ignite, true);

            checkExpire(ignite, false);
        }
    }

    /**
     * @param ignite Node.
     * @param eagerTtl Value for {@link CacheConfiguration#setEagerTtl(boolean)}.
     * @throws Exception If failed.
     */
    private void checkExpire(Ignite ignite, boolean eagerTtl) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setEagerTtl(eagerTtl);

        ignite.createCache(ccfg);

        try {
            IgniteCache<Object, Object> cache =
                ignite.cache(DEFAULT_CACHE_NAME).withExpiryPolicy(new TouchedExpiryPolicy(new Duration(0, 500)));

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 10; i++) {
                log.info("Iteration: " + i);

                AtomicInteger cntr = new AtomicInteger();

                List<Object> keys = new ArrayList<>();

                for (int j = 0; j < 10_000; j++) {
                    Object key = null;
                    Object val = null;

                    switch (rnd.nextInt(3)) {
                        case 0:
                            key = rnd.nextInt(100_000);
                            val = new TestKeyValue(cntr.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                            break;

                        case 1:
                            key = new TestKeyValue(cntr.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                            val = rnd.nextInt();
                            break;

                        case 2:
                            key = new TestKeyValue(cntr.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                            val = new TestKeyValue(cntr.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                            break;

                        default:
                            fail();
                    }

                    cache.put(key, val);

                    keys.add(key);
                }

                U.sleep(1000);

                for (Object key : keys)
                    assertNull(cache.get(key));
            }
        }
        finally {
            ignite.destroyCache(ccfg.getName());
        }
    }

    /**
     *
     */
    private static class TestKeyValue implements Serializable {
        /** */
        private int id;

        /** */
        private byte[] val;

        /**
         * @param id ID.
         * @param val Value.
         */
        TestKeyValue(int id, byte[] val) {
            this.id = id;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKeyValue that = (TestKeyValue)o;

            return id == that.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }
}
