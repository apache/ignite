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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Eviction event self test.
 */
public abstract class GridCacheEvictionEventAbstractTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    protected GridCacheEvictionEventAbstractTest() {
        super(true); // Start node.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setAtomicityMode(atomicityMode());
        cc.setEvictNearSynchronized(isNearEvictSynchronized());

        c.setCacheConfiguration(cc);

        c.setIncludeEventTypes(EVT_CACHE_ENTRY_EVICTED, EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        return c;
    }

    /**
     * @return Cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract GridCacheAtomicityMode atomicityMode();

    /**
     * @return {@code True} if near evicts synchronized.
     */
    protected boolean isNearEvictSynchronized() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictionEvent() throws Exception {
        Ignite g = grid();

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<String> oldVal = new AtomicReference<>();

        g.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                IgniteCacheEvent e = (IgniteCacheEvent) evt;

                oldVal.set((String) e.oldValue());

                latch.countDown();

                return true;
            }
        }, IgniteEventType.EVT_CACHE_ENTRY_EVICTED);

        GridCache<String, String> c = g.cache(null);

        c.put("1", "val1");

        c.evict("1");

        latch.await();

        assertNotNull(oldVal.get());
    }
}
