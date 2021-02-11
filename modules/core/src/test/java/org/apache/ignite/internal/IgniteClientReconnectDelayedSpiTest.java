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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test cases for emulation of delayed messages sending with {@link TestRecordingCommunicationSpi} for blocking and
 * resending messages at the moment we need it.
 */
public class IgniteClientReconnectDelayedSpiTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(spi);
        cfg.setCacheConfiguration(new CacheConfiguration("preconfigured-cache"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 3;
    }

    /**
     * Test checks correctness of stale {@link CacheAffinityChangeMessage} processing by client node as delayed
     * {@link GridDhtPartitionsSingleMessage} with exchId = null sends after client node reconnect happens.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectCacheDestroyedDelayedAffinityChange() throws Exception {
        Ignite ignite = ignite(1);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite);

        spi.blockMessages(GridDhtPartitionsSingleMessage.class, ignite.name());
        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return (msg instanceof GridDhtPartitionsSingleMessage) &&
                    ((GridDhtPartitionsAbstractMessage)msg).exchangeId() == null;
            }
        });

        final Ignite client = startClientGrid(getConfiguration());

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        final Ignite srv = clientRouter(client);

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(DEFAULT_CACHE_NAME);

                srv.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
            }
        });

        // Resend delayed GridDhtPartitionsSingleMessage.
        spi.waitForBlocked();
        spi.stopBlock();

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        final GridDiscoveryManager srvDisco = ((IgniteEx)srv).context().discovery();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return F.eq(true, srvDisco.cacheClientNode(client.cluster().localNode(), DEFAULT_CACHE_NAME));
            }
        }, 5000));
    }
}
