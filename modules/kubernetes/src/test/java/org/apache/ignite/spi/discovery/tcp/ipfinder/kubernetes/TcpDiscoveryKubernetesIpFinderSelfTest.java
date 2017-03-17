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

package org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes;

import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;

/**
 * TcpDiscoveryKubernetesIpFinder test.
 */
public class TcpDiscoveryKubernetesIpFinderSelfTest extends
    TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryKubernetesIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryKubernetesIpFinderSelfTest() throws Exception {
        // No-op.
    }

    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /* {@inheritDoc} */
    @Override protected TcpDiscoveryKubernetesIpFinder ipFinder() throws Exception {
        // No-op.
        return null;
    }

    /* {@inheritDoc} */
    @Override public void testIpFinder() throws Exception {
        TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder();

        ipFinder.setAccountToken(null);

        try {
            ipFinder.getRegisteredAddresses();
        }
        catch (IgniteSpiException e) {
            assertTrue(e.getMessage().startsWith("One or more configuration parameters are invalid"));
        }

        ipFinder = new TcpDiscoveryKubernetesIpFinder();

        ipFinder.setMasterUrl(null);

        try {
            ipFinder.getRegisteredAddresses();
        }
        catch (IgniteSpiException e) {
            assertTrue(e.getMessage().startsWith("One or more configuration parameters are invalid"));
        }

        ipFinder = new TcpDiscoveryKubernetesIpFinder();

        ipFinder.setNamespace(null);

        try {
            ipFinder.getRegisteredAddresses();
        }
        catch (IgniteSpiException e) {
            assertTrue(e.getMessage().startsWith("One or more configuration parameters are invalid"));
        }

        ipFinder = new TcpDiscoveryKubernetesIpFinder();

        ipFinder.setServiceName("");

        try {
            ipFinder.getRegisteredAddresses();
        }
        catch (IgniteSpiException e) {
            assertTrue(e.getMessage().startsWith("One or more configuration parameters are invalid"));
        }
    }
}