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

package org.apache.ignite.spi.communication.tcp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Contains tests for TcpCommunicationSpi that require multi JVM setup.
 */
public class TcpCommunicationSpiMultiJvmTest extends GridCommonAbstractTest {
    /** */
    private boolean remoteNodePrefersIPv4;

    /** */
    private boolean remoteNodePrefersIPv6;

    /** */
    private boolean replacingAttrSpi;

    /** */
    private String localAddrStr;

    /** */
    private InetSocketAddress externalAddr;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        if (remoteNodePrefersIPv4)
            return Collections.singletonList("-Djava.net.preferIPv4Stack=true");
        else if (remoteNodePrefersIPv6)
            return Collections.singletonList("-Djava.net.preferIPv6Stack=true");

        return Collections.emptyList();
    }

    /**
     *  Special communication SPI that replaces node's communication address in node attributes with
     *  equal IPv6 address.
     *  This enables to emulate situation with nodes on different machines bound to the same communication port.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private boolean modifyAddrAttribute;

        /** */
        public TestCommunicationSpi(boolean modifyAddrAttribute) {
            this.modifyAddrAttribute = modifyAddrAttribute;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
            Map<String, Object> attrs = super.getNodeAttributes();

            if (!modifyAddrAttribute)
                return attrs;

            attrs.put(createSpiAttributeName(ATTR_ADDRS), Arrays.asList("0:0:0:0:0:0:0:1%lo"));

            return attrs;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi commSpi = new TestCommunicationSpi(replacingAttrSpi);
        commSpi.setLocalPort(45010);

        if (localAddrStr != null)
            commSpi.setLocalAddress(localAddrStr);

        if (externalAddr != null) {

            commSpi.setAddressResolver(new AddressResolver() {
                @Override public Collection<InetSocketAddress> getExternalAddresses(
                    InetSocketAddress addr) throws IgniteCheckedException {
                    return Collections.singletonList(externalAddr);
                }
            });
        }
        else
            commSpi.setLocalAddress(InetAddress.getLocalHost().getHostName());

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Verifies that node not supporting IPv6 successfully connects and communicates with other node
     * even if that node enlists IPv6 address in its address list.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testIPv6AddressIsSkippedOnNodeNotSupportingIPv6() throws Exception {
        remoteNodePrefersIPv4 = true;
        replacingAttrSpi = true;
        localAddrStr = "127.0.0.1";
        externalAddr = new InetSocketAddress("127.0.0.1", 45010);

        Ignite ig = startGrid(0);

        localAddrStr = null;
        replacingAttrSpi = false;
        externalAddr = null;
        startGrid(1);

        AtomicBoolean cacheCreatedAndLoaded = createAndLoadCacheAsync(ig);

        assertTrue(GridTestUtils.waitForCondition(cacheCreatedAndLoaded::get, 10_000));
    }

    /**
     * Verifies that node supporting IPv6 successfully connects and communicates with node
     * supporting IPv4 only.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "java.net.preferIPv4Stack", value = "true")
    public void testIPv6NodeSuccessfullyConnectesToNodeWithIPv4Only() throws Exception {
        remoteNodePrefersIPv4 = false;
        replacingAttrSpi = false;

        IgniteEx ig = startGrid(0);

        replacingAttrSpi = true;
        localAddrStr = "127.0.0.1";
        externalAddr = new InetSocketAddress("127.0.0.1", 45010);
        startGrid(1);

        AtomicBoolean cacheCreatedAndLoaded = createAndLoadCacheAsync(ig);

        assertTrue(GridTestUtils.waitForCondition(cacheCreatedAndLoaded::get, 10_000));
    }

    /**
     * Verifies that even when two nodes explicitely declare preferring IPv6 stack they're still able to connect
     * if one node provides IPv4 address as primary one.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "java.net.preferIPv6Stack", value = "true")
    public void testIPv6NodesSuccessfullyConnectDespiteOfIPv4ExternalAddress() throws Exception {
        localAddrStr = "127.0.0.1";
        externalAddr = new InetSocketAddress("0:0:0:0:0:0:0:1%lo", 45010);
        IgniteEx ig = startGrid(0);

        remoteNodePrefersIPv6 = true;
        externalAddr = null;
        startGrid(1);

        AtomicBoolean cacheCreatedAndLoaded = createAndLoadCacheAsync(ig);
        assertTrue(GridTestUtils.waitForCondition(cacheCreatedAndLoaded::get, 10_000));
    }

    /**
     * @param ig Ignite instance used to create new cache.
     * @return AtomicBoolean to verify operation successful completion.
     */
    private AtomicBoolean createAndLoadCacheAsync(Ignite ig) {
        AtomicBoolean cacheCreatedAndLoaded = new AtomicBoolean(false);

        GridTestUtils.runAsync(() -> {
                IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

                for (int i = 0; i < 100; i++) {
                    cache.put(i, i);
                }

                cacheCreatedAndLoaded.set(true);
            }, "start_cache_thread");

        return cacheCreatedAndLoaded;
    }
}
