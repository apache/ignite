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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Test that in {@link GridCacheMode#PARTITIONED} mode cache writes values only to the near cache store. <p/> This check
 * is needed because in current implementation if {@link GridCacheWriteBehindStore} assumes that and user store is
 * wrapped only in near cache (see {@link GridCacheProcessor} init logic).
 */
@SuppressWarnings({"unchecked"})
public class GridCachePartitionedWritesTest extends GridCommonAbstractTest {
    /** Cache store. */
    private GridCacheStore store;

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        store = null;

        super.afterTest();
    }

    /** @throws Exception If test fails. */
    public void testWrite() throws Exception {
        final AtomicInteger putCnt = new AtomicInteger();
        final AtomicInteger rmvCnt = new AtomicInteger();

        store = new GridCacheStoreAdapter() {
            @Override public Object load(@Nullable IgniteTx tx, Object key) {
                info(">>> Get [key=" + key + ']');

                return null;
            }

            @Override public void put(@Nullable IgniteTx tx, Object key,
                @Nullable Object val) {
                putCnt.incrementAndGet();
            }

            @Override public void remove(@Nullable IgniteTx tx, Object key) {
                rmvCnt.incrementAndGet();
            }
        };

        startGrid();

        GridCache<Integer, String> cache = cache();

        try {
            cache.get(1);

            IgniteTx tx = cache.txStart();

            try {
                for (int i = 1; i <= 10; i++)
                    cache.putx(i, Integer.toString(i));

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert cache.size() == 10;

            assert putCnt.get() == 10;

            tx = cache.txStart();

            try {
                for (int i = 1; i <= 10; i++) {
                    String val = cache.remove(i);

                    assert val != null;
                    assert val.equals(Integer.toString(i));
                }

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert rmvCnt.get() == 10;
        }
        finally {
            stopGrid();
        }
    }
}
