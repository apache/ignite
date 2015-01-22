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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import javax.cache.*;
import javax.cache.configuration.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Checks that exception is propagated to user when cache store throws an exception.
 */
public class GridCacheGetStoreErrorSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** Cache mode for test. */
    private GridCacheMode cacheMode;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode);
        cc.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cc.setAtomicityMode(TRANSACTIONAL);

        CacheStore store = new CacheStoreAdapter<Object, Object>() {
            @Override public Object load(Object key) {
                throw new IgniteException("Failed to get key from store: " + key);
            }

            @Override public void write(Cache.Entry<?, ?> entry) {
                // No-op.
            }

            @Override public void delete(Object key) {
                // No-op.
            }
        };

        cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        c.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        return c;
    }

    /** @throws Exception If failed. */
    public void testGetErrorNear() throws Exception {
        checkGetError(true, PARTITIONED);
    }

    /** @throws Exception If failed. */
    public void testGetErrorColocated() throws Exception {
        checkGetError(false, PARTITIONED);
    }

    /** @throws Exception If failed. */
    public void testGetErrorReplicated() throws Exception {
        checkGetError(false, REPLICATED);
    }

    /** @throws Exception If failed. */
    public void testGetErrorLocal() throws Exception {
        checkGetError(false, LOCAL);
    }

    /** @throws Exception If failed. */
    private void checkGetError(boolean nearEnabled, GridCacheMode cacheMode) throws Exception {
        this.nearEnabled = nearEnabled;
        this.cacheMode = cacheMode;

        startGrids(3);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).cache(null).get(nearKey());

                    return null;
                }
            }, IgniteCheckedException.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @return Key that is not primary nor backup for grid 0. */
    private String nearKey() {
        String key = "";

        for (int i = 0; i < 1000; i++) {
            key = String.valueOf(i);

            GridCacheEntry<Object, Object> entry = grid(0).cache(null).entry(key);

            if (!entry.primary() && entry.backup())
                break;
        }

        return key;
    }
}
