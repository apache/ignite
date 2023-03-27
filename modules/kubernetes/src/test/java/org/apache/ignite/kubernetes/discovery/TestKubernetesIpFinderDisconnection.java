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

package org.apache.ignite.kubernetes.discovery;

import java.util.concurrent.CountDownLatch;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 * Test that client node is disconnected from a cluster if KubernetesIpFinder fails.
 */
public class TestKubernetesIpFinderDisconnection extends KubernetesDiscoveryAbstractTest {

    /** Tests that client is disconnected if {@link TcpDiscoverySpi#setNetworkTimeout(long)} ()} ) is set.  */
    @Test
    public void testClientNodeDisconnectsWithMaxRetries() throws Exception {
        IgniteConfiguration cfgSrv = getConfiguration(getTestIgniteInstanceName(), false);

        IgniteConfiguration cfgClient = getConfiguration("client", true);
        ((TcpDiscoverySpi)cfgClient.getDiscoverySpi()).setNetworkTimeout(1000);

        runSuccessfulDisconnectionTest(cfgSrv, cfgClient);
    }

    /** Tests that client is still trying to connect
     * if {@link TcpDiscoverySpi#setNetworkTimeout(long)} ()} ) is big. */
    @Test
    public void testClientNodeIsNotDisconnectedWithBigNetworkTimeout() throws Exception {
        IgniteConfiguration cfgSrv = getConfiguration(getTestIgniteInstanceName(), false);

        IgniteConfiguration cfgClient = getConfiguration("client", true);
        ((TcpDiscoverySpi)cfgClient.getDiscoverySpi()).setNetworkTimeout(6000);

        runFailureDisconnectionTest(cfgSrv, cfgClient);
    }

    /** Tests that client is disconnected with default settings. */
    @Test
    public void testClientNodeDisconnectsWithDefaultSettings() throws Exception {
        IgniteConfiguration cfgSrv = getConfiguration(getTestIgniteInstanceName(), false);
        IgniteConfiguration cfgClient = getConfiguration("client", true);

        runSuccessfulDisconnectionTest(cfgSrv, cfgClient);
    }

    /**
     * Runs disconnection test and check that a client is disconnected from the grid.
     */
    private void runSuccessfulDisconnectionTest(IgniteConfiguration cfgNode1, IgniteConfiguration cfgNode2) throws Exception {
        mockServerResponse();

        IgniteEx crd = startGrid(cfgNode1);
        String crdAddr = crd.localNode().addresses().iterator().next();

        mockServerResponse(crdAddr);

        IgniteEx client = startGrid(cfgNode2);

        waitForTopology(2);

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertTrue("Failed to wait for client node disconnected.", latch.await(8, SECONDS));
    }

    /**
     * Runs disconnection test and check that a client is connected to the grid.
     */
    private void runFailureDisconnectionTest(IgniteConfiguration cfgNode1, IgniteConfiguration cfgNode2) throws Exception {
        mockServerResponse();

        IgniteEx crd = startGrid(cfgNode1);
        String crdAddr = crd.localNode().addresses().iterator().next();

        mockServerResponse(crdAddr);

        IgniteEx client = startGrid(cfgNode2);

        waitForTopology(2);

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertFalse("A client should not be disconnected.", latch.await(5, SECONDS));
    }
}
