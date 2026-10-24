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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that a warning is printed when the IP finder has a long list of addresses (see IGNITE-1025).
 */
public class TcpDiscoveryLongAddressListWarnSelfTest extends GridCommonAbstractTest {
    /** Base port for unavailable addresses. */
    private static final int UNAVAILABLE_PORT = 49_152;

    /** */
    private GridStringLogger strLog;

    /** */
    private int addrCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        List<String> addrs = new ArrayList<>(addrCnt);

        for (int i = 0; i < addrCnt; i++)
            addrs.add("127.0.0.1:" + (UNAVAILABLE_PORT + i));

        TcpDiscoverySpi spi = new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder(true).setAddresses(addrs))
            .setJoinTimeout(1_000);

        cfg.setDiscoverySpi(spi);
        cfg.setGridLogger(strLog);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWarnOnLongAddressList() throws Exception {
        strLog = new GridStringLogger();

        addrCnt = TcpDiscoverySpi.ADDRS_WARN_THRESHOLD + 1;

        startGrid(0);

        String log = strLog.toString();

        assertTrue("Warning must be printed to log: " + log,
            log.contains("Long list of addresses resolved from IP finder"));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoWarnOnShortAddressList() throws Exception {
        strLog = new GridStringLogger();

        addrCnt = 3;

        startGrid(0);

        String log = strLog.toString();

        assertFalse("Warning must not be printed to log: " + log,
            log.contains("Long list of addresses resolved from IP finder"));

        stopAllGrids();
    }
}
