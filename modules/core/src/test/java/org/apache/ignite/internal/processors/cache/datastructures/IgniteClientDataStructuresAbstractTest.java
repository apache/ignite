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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

/**
 *
 */
public abstract class IgniteClientDataStructuresAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODE_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (gridName.equals(getTestGridName(NODE_CNT - 1))) {
            cfg.setClientMode(true);

            if (clientDiscovery()) {
                TcpDiscoveryVmIpFinder clientFinder = new TcpDiscoveryVmIpFinder();

                String firstSrvAddr = F.first(ipFinder.getRegisteredAddresses()).toString();

                if (firstSrvAddr.startsWith("/"))
                    firstSrvAddr = firstSrvAddr.substring(1);

                clientFinder.setAddresses(Collections.singletonList(firstSrvAddr));

                TcpClientDiscoverySpi discoverySpi = new TcpClientDiscoverySpi();
                discoverySpi.setIpFinder(clientFinder);

                cfg.setDiscoverySpi(discoverySpi);
            }
        }

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return {@code True} if use client discovery.
     */
    protected abstract boolean clientDiscovery();

    /**
     * @throws Exception If failed.
     */
    public void testSequence() throws Exception {
        Ignite clientNode = clientIgnite();

        Ignite srvNode = serverNode();

        assertNull(clientNode.atomicSequence("seq1", 1L, false));

        try (IgniteAtomicSequence seq = clientNode.atomicSequence("seq1", 1L, true)) {
            assertNotNull(seq);

            assertEquals(1L, seq.get());

            assertEquals(1L, seq.getAndAdd(1));

            assertEquals(2L, seq.get());

            IgniteAtomicSequence seq0 = srvNode.atomicSequence("seq1", 1L, false);

            assertNotNull(seq0);
        }

        assertNull(clientNode.atomicSequence("seq1", 1L, false));
        assertNull(srvNode.atomicSequence("seq1", 1L, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLong() throws Exception {
        Ignite clientNode = clientIgnite();

        Ignite srvNode = serverNode();

        assertNull(clientNode.atomicLong("long1", 1L, false));

        try (IgniteAtomicLong cntr = clientNode.atomicLong("long1", 1L, true)) {
            assertNotNull(cntr);

            assertEquals(1L, cntr.get());

            assertEquals(1L, cntr.getAndAdd(1));

            assertEquals(2L, cntr.get());

            IgniteAtomicLong cntr0 = srvNode.atomicLong("long1", 1L, false);

            assertNotNull(cntr0);

            assertEquals(2L, cntr0.get());

            assertEquals(3L, cntr0.incrementAndGet());

            assertEquals(3L, cntr.get());
        }

        assertNull(clientNode.atomicLong("long1", 1L, false));
        assertNull(srvNode.atomicLong("long1", 1L, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        Ignite clientNode = clientIgnite();

        Ignite srvNode = serverNode();

        assertNull(clientNode.set("set1", null));

        CollectionConfiguration colCfg = new CollectionConfiguration();

        try (IgniteSet<Integer> set = clientNode.set("set1", colCfg)) {
            assertNotNull(set);

            assertEquals(0, set.size());

            assertFalse(set.contains(1));

            assertTrue(set.add(1));

            assertTrue(set.contains(1));

            IgniteSet<Integer> set0 = srvNode.set("set1", null);

            assertTrue(set0.contains(1));

            assertEquals(1, set0.size());

            assertTrue(set0.remove(1));

            assertFalse(set.contains(1));
        }
    }

    /**
     * @return Client node.
     */
    private Ignite clientIgnite() {
        Ignite ignite = ignite(NODE_CNT - 1);

        assertTrue(ignite.configuration().isClientMode());

        if (clientDiscovery())
            assertTrue(ignite.configuration().getDiscoverySpi() instanceof TcpClientDiscoverySpi);
        else
            assertTrue(ignite.configuration().getDiscoverySpi() instanceof TcpDiscoverySpi);

        return ignite;
    }

    /**
     * @return Server node.
     */
    private Ignite serverNode() {
        Ignite ignite = ignite(0);

        assertFalse(ignite.configuration().isClientMode());

        return ignite;
    }
}
