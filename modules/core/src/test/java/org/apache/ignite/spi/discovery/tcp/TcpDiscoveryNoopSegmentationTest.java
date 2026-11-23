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

package org.apache.ignite.spi.discovery.tcp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;

/**
 * Discovery operations on a server node that was segmented with {@link SegmentationPolicy#NOOP}: they have to complete
 * although the node's discovery SPI is disconnected.
 */
public class TcpDiscoveryNoopSegmentationTest extends GridCommonAbstractTest {
    /** Timeout of an operation that must not wait for the cluster. */
    private static final long TIMEOUT = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSegmentationPolicy(SegmentationPolicy.NOOP)
            .setIncludeEventTypes(EVT_NODE_SEGMENTED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Closes a continuous query after the segmentation, as an application does when it shuts down.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCloseContinuousQueryAfterSegmentation() throws Exception {
        startGrids(3);

        IgniteEx victim = grid(2);

        QueryCursor<Cache.Entry<Object, Object>> cur = victim.getOrCreateCache(DEFAULT_CACHE_NAME)
            .query(new ContinuousQuery<>().setLocalListener(evts -> { }));

        segment(victim);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(cur::close);

        fut.get(TIMEOUT);
    }

    /**
     * Closes a continuous query inside the {@code EVT_NODE_SEGMENTED} listener, while the discovery SPI is still
     * connected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCloseContinuousQueryInSegmentationListener() throws Exception {
        startGrids(3);

        IgniteEx victim = grid(2);

        QueryCursor<Cache.Entry<Object, Object>> cur = victim.getOrCreateCache(DEFAULT_CACHE_NAME)
            .query(new ContinuousQuery<>().setLocalListener(evts -> { }));

        CountDownLatch closed = new CountDownLatch(1);

        victim.events().localListen(evt -> {
            cur.close();

            closed.countDown();

            return false;
        }, EVT_NODE_SEGMENTED);

        grid(0).configuration().getDiscoverySpi().failNode(victim.localNode().id(), null);

        assertTrue("Continuous query was not closed", closed.await(TIMEOUT, TimeUnit.MILLISECONDS));
    }

    /**
     * Starts a cache after the segmentation: the discovery custom message cannot be sent any more.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCacheAfterSegmentation() throws Exception {
        startGrids(3);

        IgniteEx victim = grid(2);

        segment(victim);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> victim.getOrCreateCache("new-cache"));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteSpiException.class,
            "Failed to send custom message");
    }

    /**
     * Makes another node fail {@code victim}; the victim segments itself and, with {@link SegmentationPolicy#NOOP},
     * disconnects its discovery SPI.
     *
     * @param victim Node to segment.
     * @throws Exception If failed.
     */
    private void segment(IgniteEx victim) throws Exception {
        CountDownLatch segmented = new CountDownLatch(1);

        victim.events().localListen(evt -> {
            segmented.countDown();

            return false;
        }, EVT_NODE_SEGMENTED);

        grid(0).configuration().getDiscoverySpi().failNode(victim.localNode().id(), null);

        assertTrue("Node was not segmented", segmented.await(TIMEOUT, TimeUnit.MILLISECONDS));

        TcpDiscoverySpi spi = (TcpDiscoverySpi)victim.configuration().getDiscoverySpi();

        assertTrue("Discovery SPI was not disconnected", GridTestUtils.waitForCondition(
            () -> GridTestUtils.getFieldValue(spi.impl, "spiState") == TcpDiscoverySpiState.DISCONNECTED, TIMEOUT));
    }
}
