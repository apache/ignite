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

package org.apache.ignite.spi.discovery.tcp.ipfinder.vm;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * GridTcpDiscoveryVmIpFinder test.
 */
public class TcpDiscoveryVmIpFinderSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryVmIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryVmIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryVmIpFinder ipFinder() {
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

        assert !finder.isShared() : "Ip finder should NOT be shared by default.";

        return finder;
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testAddressesInitialization() throws Exception {
        TcpDiscoveryVmIpFinder finder = ipFinder();

        try {
            finder.setAddresses(Arrays.asList("127.0.0.1:475000001"));

            assert false;
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);

            assert e.getMessage().contains("127.0.0.1:475000001");
        }

        try {
            finder.setAddresses(Arrays.asList("127.0.0.1:-2"));

            assert false;
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);

            assert e.getMessage().contains("127.0.0.1:-2");
        }

        finder.setAddresses(Arrays.asList("127.0.0.1:45555", "8.8.8.8", "some-dns-name", "some-dns-name1:200",
            "127.0.0.1:"));

        info("IP finder initialized: " + finder);

        assert finder.getRegisteredAddresses().size() == 5;

        finder = ipFinder();

        try {
            finder.setAddresses(Collections.singleton("127.0.0.1:555..444"));

            assert false;
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("127.0.0.1:0..444"));

            assert false;
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("127.0.0.1:-8080..-80"));

            assert false;
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        finder.setAddresses(Collections.singleton("127.0.0.1:47500..47509"));

        assert finder.getRegisteredAddresses().size() == 10 : finder.getRegisteredAddresses();
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testIpV6AddressesInitialization() throws Exception {
        TcpDiscoveryVmIpFinder finder = ipFinder();

        try {
            finder.setAddresses(Arrays.asList("[::1]:475000001"));

            fail();
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Arrays.asList("[::1]:-2"));

            fail();
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        finder.setAddresses(Arrays.asList("[::1]:45555", "8.8.8.8", "some-dns-name", "some-dns-name1:200", "::1"));

        info("IP finder initialized: " + finder);

        assertEquals(5, finder.getRegisteredAddresses().size());

        finder = ipFinder();

        try {
            finder.setAddresses(Collections.singleton("[::1]:555..444"));

            fail();
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("[::1]:0..444"));

            fail();
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        try {
            finder.setAddresses(Collections.singleton("[::1]:-8080..-80"));

            fail();
        }
        catch (IgniteSpiException e) {
            info("Caught expected exception: " + e);
        }

        finder.setAddresses(Collections.singleton("0:0:0:0:0:0:0:1"));

        assertEquals(1, finder.getRegisteredAddresses().size());

        finder.setAddresses(Collections.singleton("[0:0:0:0:0:0:0:1]"));

        assertEquals(1, finder.getRegisteredAddresses().size());

        finder.setAddresses(Collections.singleton("[0:0:0:0:0:0:0:1]:47509"));

        assertEquals(1, finder.getRegisteredAddresses().size());

        finder.setAddresses(Collections.singleton("[::1]:47500..47509"));

        assertEquals("Registered addresses: " + finder.getRegisteredAddresses().toString(),
            10, finder.getRegisteredAddresses().size());
    }

    /**
     *
     */
    @Test
    public void testUnregistration() throws Exception {
        Ignition.start(config("server1", false, false));

        int srvSize = sharedStaticIpFinder.getRegisteredAddresses().size();

        Ignition.start(config("server2", false, false));
        Ignition.start(config("client1", true, false));

        assertEquals(2 * srvSize, sharedStaticIpFinder.getRegisteredAddresses().size());

        Ignition.start(config("client2", true, false));
        Ignition.start(config("client3", true, false));

        assertEquals(2 * srvSize, sharedStaticIpFinder.getRegisteredAddresses().size());

        Ignition.start(config("client4", true, true));

        assertEquals(3 * srvSize, sharedStaticIpFinder.getRegisteredAddresses().size());

        Ignition.stop("client1", true);
        Ignition.stop("client2", true);
        Ignition.stop("client3", true);

        assertEquals(3 * srvSize, sharedStaticIpFinder.getRegisteredAddresses().size());

        Ignition.stop("client4", true);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return 2 == G.allGrids().size();
            }
        }, 10000);

        Ignition.stop("server1", true);
        Ignition.stop("server2", true);

        boolean res = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return G.allGrids().isEmpty();
            }
        }, 10000);

        assertTrue(res);

        assertTrue(3 * srvSize >= sharedStaticIpFinder.getRegisteredAddresses().size());
    }

    /**
     * @param name Name.
     * @param client Client.
     */
    private static IgniteConfiguration config(String name, boolean client, boolean forceServerMode) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);
        cfg.setClientMode(client);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setForceServerMode(forceServerMode);
        disco.setIpFinder(sharedStaticIpFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }
}
