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

import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * Checks that exception is propagated to user when cache store throws an exception.
 */
public class GridCacheGetStoreErrorSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** Cache mode for test. */
    private CacheMode cacheMode;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode);

        if (nearEnabled)
            cc.setNearConfiguration(new NearCacheConfiguration());

        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setCacheStoreFactory(new IgniteReflectionFactory<CacheStore>(TestStore.class));
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
    private void checkGetError(boolean nearEnabled, CacheMode cacheMode) throws Exception {
        this.nearEnabled = nearEnabled;
        this.cacheMode = cacheMode;

        startGrids(3);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).cache(null).get(nearKey());

                    return null;
                }
            }, CacheLoaderException.class, null);
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

            if (!grid(0).affinity(null).isPrimaryOrBackup(grid(0).localNode(), key))
                break;
        }

        return key;
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        @Override public Object load(Object key) {
            throw new IgniteException("Failed to get key from store: " + key);
        }

        @Override public void write(Cache.Entry<?, ?> entry) {
            // No-op.
        }

        @Override public void delete(Object key) {
            // No-op.
        }
    }
}