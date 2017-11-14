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
    private List<String> ipAddrList = new ArrayList<>();

    /** Warning message that will be in log. */
    private String warningMsg = "Unavailabe ip address in Windows OS [%s]." +
        " Connection can take a lot of time. If there are any other addresses, " +
        "check your address list in ipFinder.";

    /** Right ip address. */
    private String rightAddr = "127.0.0.1";

    /** Wrong ip address. */
    private String wrongAddr = "0.0.0.0";

    /** Ip finder. */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        ipAddrList.clear();

        strLog.reset();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        boolean skipTimeout = igniteInstanceName.contains("wrongAddress") || igniteInstanceName.contains("SmallSocketTimeout");

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        if (igniteInstanceName.contains("client")) {
            if (skipTimeout)
                spi.setJoinTimeout(1);
            cfg.setCacheConfiguration();
            cfg.setClientMode(true);
        }
        else
            spi.setForceServerMode(true);

        if (igniteInstanceName.contains("RightAddress"))
            ipAddrList.add(rightAddr + ":47501");

        ipFinder.setAddresses(ipAddrList);

        spi.setIpFinder(ipFinder);

        cfg.setGridLogger(strLog);

        cfg.setDiscoverySpi(spi);

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
        logWarning = String.format(warningMsg, wrongAddr);

        ipAddrList.add(wrongAddr);

        try {
            startGrid("wrongAddress-client");
        }
        catch (IgniteCheckedException e) {
            //because of absent available server node for this client
        }

        finally {
            assertTrue(strLog.toString().contains(logWarning));
        }
    }

    /**
     *
     */
    public void testRightAddressClientMode() throws Exception {
        logWarning = String.format(warningMsg, rightAddr);

        startGrid("firstNode");

        strLog.reset();

        startGrid("RightAddress-client");

        assertFalse(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testRightAddresses() throws Exception {
        logWarning = String.format(warningMsg, rightAddr);

        ipAddrList.add(rightAddr);

        startGrid("firstNode");

        strLog.reset();

        startGrid("RightAddress-default");

        assertFalse(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testWrongAddresses() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr);

        ipAddrList.add(wrongAddr);

        startGrid("default");

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testSmallSocketTimeout() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr);

        ipAddrList.add(wrongAddr);

        startGrid("SmallSocketTimeout");

        assertTrue(strLog.toString().contains(logWarning));
    }

    /**
     *
     */
    public void testClientSmallSocketTimeout() throws Exception {
        logWarning = String.format(warningMsg, wrongAddr);

        ipAddrList.add(wrongAddr);

        try {
            startGrid("SmallSocketTimeout-client");
        }
        catch (IgniteCheckedException e) {
            //because of absent available server node for this client
        }

        finally {
            assertTrue(strLog.toString().contains(logWarning));
        }
    }
}