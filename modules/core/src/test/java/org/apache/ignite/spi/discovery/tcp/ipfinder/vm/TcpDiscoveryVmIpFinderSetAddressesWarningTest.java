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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test printing warning in setAddresses when there is at least one wrong ip address.
 */
public class TcpDiscoveryVmIpFinderSetAddressesWarningTest extends GridCommonAbstractTest {
    /** Logger warning. */
    private String logWarning;

    /** String logger. */
    private GridStringLogger strLog = new GridStringLogger();

    /** List of ip addresses. */
    private List<String> sockAddrList = new ArrayList<>();

    /** Warning message that will be in log. */
    private static final String warningMsg = "Unavailable socket address in Windows OS [%s]. " +
        "Connection can take a lot of time. Maybe the reason is that the node has not been started yet on this " +
        "address. If there are any other addresses, check your address list in ipFinder.";

    /** Right ip address. */
    private static final String rightAddr = "127.0.0.1";

    /** Right port. */
    private static final String rightPort = ":47501";

    /** Wrong ip address. */
    private static final String wrongAddr = "0.0.0.0";

    /** Wrong port. */
    private static final String wrongPort = ":8080";

    /** Ip finder. */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        boolean skipTimeout = igniteInstanceName.contains("wrongAddress") ||
            igniteInstanceName.contains("SmallSocketTimeout");

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        if (igniteInstanceName.contains("client")) {
            if (skipTimeout)
                spi.setJoinTimeout(1);
            cfg.setClientMode(true);
        }

        if (igniteInstanceName.contains("RightAddress"))
            sockAddrList.add(rightAddr + rightPort);

        ipFinder.setAddresses(sockAddrList);

        spi.setIpFinder(ipFinder);

        cfg.setGridLogger(strLog);

        if (igniteInstanceName.contains("SmallSocketTimeout"))
            spi.setSocketTimeout(500);

        if (igniteInstanceName.equals("firstNode"))
            spi.setLocalPort(47501);

        return cfg;
    }

    /**
     *
     */
    public void testWrongAddressClientMode() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr + wrongPort);

        sockAddrList.add(wrongAddr + wrongPort);

        try {
            startGrid("wrongAddress-client");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.hasCause(IgniteSpiException.class));
        }

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testRightAddressWrongPortClientMode() throws Exception {
        logWarning = String.format(warningMsg, rightAddr + wrongPort);

        sockAddrList.add(rightAddr + wrongPort);

        try {
            startGrid("wrongAddress-client");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.hasCause(IgniteSpiException.class));
        }

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testRightAddressClientMode() throws Exception {
        logWarning = String.format(warningMsg, rightAddr + rightPort);

        startGrid("firstNode");

        strLog.reset();

        startGrid("RightAddress-client");

        assertFalse(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testWrongAddresses() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr + wrongPort);

        sockAddrList.add(wrongAddr + wrongPort);

        startGrid("default");

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testRightAddressesWrongPort() throws Exception {
        logWarning = String.format(warningMsg, rightAddr + wrongPort);

        sockAddrList.add(rightAddr + wrongPort);

        startGrid("default");

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testRightAddresses() throws Exception {
        logWarning = String.format(warningMsg, rightAddr + rightPort);

        sockAddrList.add(rightAddr + rightPort);

        startGrid("firstNode");

        strLog.reset();

        startGrid("RightAddress-default");

        assertFalse(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testSmallSocketTimeout() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr + wrongPort);

        sockAddrList.add(wrongAddr + wrongPort);

        startGrid("SmallSocketTimeout");

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testClientSmallSocketTimeout() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr + wrongPort);

        sockAddrList.add(wrongAddr + wrongPort);

        try {
            startGrid("SmallSocketTimeout-client");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.hasCause(IgniteSpiException.class));
        }

        assertTrue(strLog.toString().contains(logWarning));
    }

}