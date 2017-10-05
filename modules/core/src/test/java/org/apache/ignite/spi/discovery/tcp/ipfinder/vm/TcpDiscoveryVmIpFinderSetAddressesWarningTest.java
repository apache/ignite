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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
　
/**
 * Test printing warning in setAddresses when there is at least one wrong ip address.
 */
public class TcpDiscoveryVmIpFinderSetAddressesWarningTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryVmIpFinder> {
    private GridStringLogger strLog = new GridStringLogger();
　
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryVmIpFinderSetAddressesWarningTest() throws Exception {
        // No-op.
    }
　
    /** {@inheritDoc} */
    @Override protected TcpDiscoveryVmIpFinder ipFinder() {
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
　
        assert !finder.isShared() : "Ip finder should NOT be shared by default.";
　
        return finder;
    }
　
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
　
//        cfg.setGridLogger(strLog = new GridStringLogger());
　
        return cfg;
    }
　
    /**
     * Set several sddresses to ipFinder. Fail if it's at least one wrong address.
     *
     * @throws Exception If any error occurs.
     */
    public void testWrongIpAddressesSetting() throws Exception {
        String wrongAddr1 = "527.0.0.1:45555";
　
        String wrongAddr2 = "some-dns-name";
　
        GridTestUtils.setFieldValue(finder, "log", strLog);
　
        finder.setAddresses(Arrays.asList(wrongAddr1, "8.8.8.8", wrongAddr2, "127.0.0.1:"));
　
        assertTrue(strLog.toString().contains(wrongAddr1));
　
        assertTrue(!strLog.toString().contains(wrongAddr2));
    }
　
    /**
     * Set address with several ports to ipFinder. Fail if it's at least one wrong address.
     *
     * @throws Exception If any error occurs.
     */
    public void testMultiWrongIpAddressesSetting() throws Exception {
        String wrongAddrMultiPort = "727.0.0.1:47500..47509";
　
        GridTestUtils.setFieldValue(finder, "log", strLog);
　
        finder.setAddresses(Collections.singleton(wrongAddrMultiPort));
　
        assertTrue(strLog.toString().contains("727.0.0.1:47500"));
　
        assertTrue(!strLog.toString().contains("727.0.0.1:47501"));
    }
}
　
