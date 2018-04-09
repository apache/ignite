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

package org.apache.ignite.spi.discovery.tcp.ipfinder.consul;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteIgnore;
import org.apache.ignite.testsuites.IgniteConsulTestSuite;

/**
 * Abstract TcpDiscoveryConsulIpFinder to test with different ways of setting AWS credentials.
 */
abstract class TcpDiscoveryConsulIpFinderAbstractSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryConsulIpFinder> {

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    TcpDiscoveryConsulIpFinderAbstractSelfTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryConsulIpFinder ipFinder() throws Exception {
        TcpDiscoveryConsulIpFinder finder = new TcpDiscoveryConsulIpFinder();

        injectLogger(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";



        for (int i = 0; i < 5; i++) {
            Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

            if (!addrs.isEmpty())
                finder.unregisterAddresses(addrs);
            else
                return finder;

            U.sleep(1000);
        }

        if (!finder.getRegisteredAddresses().isEmpty())
            throw new Exception("Failed to initialize IP finder.");

        return finder;
    }

    /** {@inheritDoc} */
    @IgniteIgnore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Override public void testIpFinder() throws Exception {
        super.testIpFinder();
    }


    protected void setHostUrl(TcpDiscoveryConsulIpFinder finder) {
        finder.setHostUrl(getHostUrl());
    }

    /**
     * Gets Bucket name.
     * Bucket name should be unique for the host to parallel test run on one bucket.
     * Please note that the final bucket name should not exceed 63 chars.
     *
     * @return Bucket name.
     */
    static String getHostUrl() {
        String hostUrl;
        try {
            hostUrl = IgniteConsulTestSuite.getHostUrl(
                "ip-finder-unit-test-" + InetAddress.getLocalHost().getHostName().toLowerCase());
        }
        catch (UnknownHostException e) {
            hostUrl = IgniteConsulTestSuite.getHostUrl(
                "ip-finder-unit-test-rnd-" + ThreadLocalRandom.current().nextInt(100));
        }

        return hostUrl;
    }
}