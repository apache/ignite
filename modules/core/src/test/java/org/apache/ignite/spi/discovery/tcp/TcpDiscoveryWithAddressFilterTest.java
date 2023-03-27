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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

/**
 * Test discovery SPI with address filter present
 */
public class TcpDiscoveryWithAddressFilterTest extends TcpDiscoveryWithWrongServerTest {
    /** Address filter predicate which allows filtering IP addresses duringd discovery */
    private IgnitePredicate<InetSocketAddress> addressFilter = new P1<InetSocketAddress>() {
        @Override public boolean apply(InetSocketAddress address) {
            // Compile regular expression
            Pattern pattern = Pattern.compile("^/127\\.0\\.0\\.1:47503$", Pattern.CASE_INSENSITIVE);
            // Match regex against input
            Matcher matcher = pattern.matcher(address.toString());
            // Use results...
            return !(matcher.matches());
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singleton("127.0.0.1:" + SERVER_PORT + ".." + LAST_SERVER_PORT));

        cfg.setDiscoverySpi(new TcpDiscoveryWithAddressFilter().setIpFinder(ipFinder).setAddressFilter(addressFilter));

        return cfg;
    }

    /**
     * Basic test
     */
    @Test
    public void testBasic() throws Exception {
        startTcpThread(new NoResponseWorker(), SERVER_PORT);
        startTcpThread(new NoResponseWorker(), LAST_SERVER_PORT);

        simpleTest();
    }

    /**
     * Check for incoming addresses and check that the filter was applied
     */
    private class TcpDiscoveryWithAddressFilter extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected Collection<InetSocketAddress> resolvedAddresses() throws IgniteSpiException {
            Collection<InetSocketAddress> res = super.resolvedAddresses();

            for (InetSocketAddress addr : res)
                assertFalse(addr.getHostName().matches("^/127\\.0\\.0\\.1:47503$"));

            return res;
        }
    }
}
