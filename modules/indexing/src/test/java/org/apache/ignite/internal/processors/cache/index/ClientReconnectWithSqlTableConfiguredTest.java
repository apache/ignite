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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 * Tests for checking reconnect client with SQL caches.
 */
public class ClientReconnectWithSqlTableConfiguredTest extends AbstractIndexingCommonTest {
    /** True means that the caches belong a same group, or false otherwise. */
    private boolean sameGrp;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /**
     * Creates a configuration for cache with specified name.
     *
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration createCacheConfiguration(String cacheName) {
        return new CacheConfiguration(cacheName)
            .setCacheMode(CacheMode.REPLICATED)
            .setGroupName(sameGrp ? "grp" : "grp-" + cacheName)
            .setQueryEntities(Arrays.asList(new QueryEntity()
                .setTableName("TEST-" + cacheName)
                .setKeyType(String.class.getName())
                .setValueType(java.sql.Timestamp.class.getName())
            ));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Check a case when caches belong to one group.
     *
     * @throws Exception If fiald.
     */
    @Test
    public void testCacheInOneGroup() throws Exception {
        sameGrp = true;

        checkClientRecconect();
    }

    /**
     * Check a case when caches belong to different groups.
     *
     * @throws Exception If faild.
     */
    @Test
    public void testCacheInDifferentGroups() throws Exception {
        sameGrp = false;

        checkClientRecconect();
    }

    /**
     * Checks that no exception after reconnect client to cluster.
     *
     * @throws Exception If failed.
     */
    private void checkClientRecconect() throws Exception {
        IgniteEx ignite0 = startGrids(2);

        IgniteEx client1 = startClientGrid(2);
        IgniteEx client2 = startClientGrid(3);

        ignite0.cluster().active(true);

        client1.getOrCreateCache(createCacheConfiguration("test-cl1"));
        client2.getOrCreateCache(createCacheConfiguration("test-cl2"));

        client2.close();

        stopAllServers(true);

        CountDownLatch reconnectLatch = new CountDownLatch(1);

        client1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info(">>>>> reconnect evt=" + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_RECONNECTED);

        ignite0 = startGrid(0);

        reconnectLatch.await();

        startGrid(1);

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(3, 1);

        AtomicReference<GridDhtPartitionsExchangeFuture> lastFinishedFuture = new AtomicReference<>();

        assertTrue("Could not wait for autoactivation.", GridTestUtils.waitForCondition(() -> {
            for (GridDhtPartitionsExchangeFuture fut : client1.context().cache().context().exchange().exchangeFutures()) {
                if (fut.isDone() && fut.topologyVersion().equals(topVer)) {
                    lastFinishedFuture.set(fut);
                    return true;
                }
            }
            return false;
        }, 15_000));

        log.info(">>>>> lastFinishedFuture ver=" + lastFinishedFuture.get().topologyVersion());

        Throwable t = U.field(lastFinishedFuture.get(), "exchangeLocE");

        assertNull("Unexpected exception on client node [exc=" + t + ']', t);
    }
}
