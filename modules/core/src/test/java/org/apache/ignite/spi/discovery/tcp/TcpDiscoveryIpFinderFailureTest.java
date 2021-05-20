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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Check different disconnections scenarios in respect to {@link TcpDiscoverySpi#joinTimeout} cfg
 * alongside possible failures in IpResolver.
 */
public class TcpDiscoveryIpFinderFailureTest extends GridCommonAbstractTest {

    /**  */
    private TestDynamicIpFinder dynamicIpFinder;

    /** Listening test logger. */
    private ListeningTestLogger listeningLog;

    /** */
    @Before
    public void initDynamicIpFinder() {
        dynamicIpFinder = new TestDynamicIpFinder();
        listeningLog = new ListeningTestLogger(log);
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /**
     * Tests shared client node disconnection should use {@link TcpDiscoverySpi#netTimeout).
     */
    @Test
    public void testClientNodeSharedIpFinderFailure() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        runClientNodeIpFinderFailureTest(TcpDiscoverySpi.DFLT_RECONNECT_DELAY);
    }

    /**
     * Tests static client node disconnection should use {@link TcpDiscoverySpi#netTimeout).
     */
    @Test
    public void testClientNodeStaticIpFinderFailure() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));
        dynamicIpFinder.setShared(false);
        runClientNodeIpFinderFailureTest(TcpDiscoverySpi.DFLT_RECONNECT_DELAY);
    }

    /**
     * Tests client node disconnection works with {@link TcpDiscoverySpi#getReconnectDelay()} ) set to zero.
     */
    @Test
    public void testClientNodeIpFinderFailureWithZeroReconnectDelay() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        runClientNodeIpFinderFailureTest(0);
    }

    /** */
    private void runClientNodeIpFinderFailureTest(long reconnectDelay) throws Exception {
        List<LogListener> listeners = new ArrayList<>();

        listeners.add(LogListener.matches(
                "Failed to get registered addresses from IP " +
                        "finder (retrying every " + reconnectDelay + "ms; change 'reconnectDelay' to configure " +
                        "the frequency of retries) [maxTimeout=4000]").build());

        listeners.add(LogListener.matches(
                "Unable to get registered addresses from IP finder," +
                        " timeout is reached (consider increasing 'joinTimeout' for join process or " +
                        "'netTimeout' for reconnection) [joinTimeout=10000, netTimeout=4000]").build());

        listeners.forEach(listeningLog::registerListener);

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server", false);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setJoinTimeout(10000);
        discoverySpi.setNetworkTimeout(4000);
        discoverySpi.setReconnectDelay((int) reconnectDelay);
        discoverySpi.setIpFinder(dynamicIpFinder);

        IgniteConfiguration cfgClient = getConfigurationDynamicIpFinder("Client", true, discoverySpi);

        IgniteEx crd = startGrid(cfgSrv);
        IgniteEx client = startGrid(cfgClient);

        waitForTopology(2);

        dynamicIpFinder.breakService();

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            listeners.forEach(lsnr -> assertTrue(lsnr.check()));

            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertTrue("Failed to wait for client node disconnected.", latch.await(6, SECONDS));
    }

    /**
     * Tests server node disconnection with dynamic IP finder.
     * Server node should catch NODE_LEFT event, no calls to IP resolver.
     */
    @Test
    public void testServerNodeDynamicIpFinderFailureInTheMiddle() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        runServerNodeDisconnectionITest();
    }

    /**
     * Tests server node disconnection with static IP finder.
     * Server node should catch NODE_LEFT event, no calls to IP resolver.
     */
    @Test
    public void testServerNodeStaticIpFinderFailureInTheMiddle() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));
        dynamicIpFinder.setShared(false);

        runServerNodeDisconnectionITest();
    }

    /** */
    private void runServerNodeDisconnectionITest() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false);
        IgniteConfiguration cfgSrv2 = getConfigurationDynamicIpFinder("Server2", false);

        IgniteEx crd = startGrid(cfgSrv);
        IgniteEx srv = startGrid(cfgSrv2);

        waitForTopology(2);

        dynamicIpFinder.breakService();

        CountDownLatch latch = new CountDownLatch(1);

        srv.events().localListen(event -> {
            latch.countDown();
            return true;
        }, EVT_NODE_LEFT);

        Ignition.stop(crd.name(), true);

        assertTrue("Failed to wait for server node disconnected.", latch.await(10, SECONDS));
    }

    /**
     * Tests that dynamic IP finder doesn't allow server node to join topology if IpResolver is unavailable
     * and joinTimeout is not specified.
     * Server node should be constantly trying to obtain IP addresses.
     */
    @Test
    public void testServerNodeBrokenDynamicIpFinderFromTheStartNoJoinTimeout() throws Exception {
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false, 0);

        dynamicIpFinder.breakService();

        AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(cfgSrv);

                fail("Server node should not join a cluster if dynamic service is not working");
            }
            catch (IgniteCheckedException e) {
                if (X.hasCause(e, IgniteSpiException.class))
                    return true;
            }
            catch (Exception e) {
                return false;
            }
            finally {
                done.set(true);
            }

            return false;
        });

        if (!GridTestUtils.waitForCondition(done::get, 10_000)) {
            fut.cancel();

            Assert.assertEquals("Node was not failed", fut.get(), true);
        } else {
            String nodeState = fut.get() ? "Connected" : "Failed";

            fail("Node should be still trying to join topology. State=" + nodeState);
        }
    }

    /**
     * Tests that static IP finder doesn't allow server node to join topology if IpResolver is unavailable
     * and joinTimeout is set to 0
     * Server node should be constantly trying to obtain IP addresses.
     */
    @Test
    public void testServerNodeBrokenStaticIpFinderZeroJoinTimeout() throws Exception {
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false, 0);
        dynamicIpFinder.setShared(false);
        dynamicIpFinder.breakService();

        AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(cfgSrv);

                fail("Server node should not join a cluster if static ip finder is not working");
            }
            catch (IgniteCheckedException e) {
                if (X.hasCause(e, IgniteSpiException.class))
                    return true;
            }
            catch (Exception e) {
                return false;
            }
            finally {
                done.set(true);
            }

            return false;
        });

        if (!GridTestUtils.waitForCondition(() -> done.get(), 5_000)) fut.cancel();

        Assert.assertEquals("Node should be stuck in a joining loop", fut.get(), true);
    }

    /**
     * Tests that broken static IP finder allows server node to join topology
     * when joinTimeout expires.
     */
    @Test
    public void testServerNodeBrokenStaticIpFinderWithJoinTimeout() throws Exception {
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false, 2000);

        dynamicIpFinder.breakService();

        List<LogListener> listeners = new ArrayList<>();

        listeners.add(LogListener.matches(
                "Failed to get registered addresses from IP " +
                        "finder (retrying every 2000ms; change 'reconnectDelay' to configure " +
                        "the frequency of retries) [maxTimeout=2000]").build());

        listeners.add(LogListener.matches(
                "Unable to get registered addresses from IP finder," +
                        " timeout is reached (consider increasing 'joinTimeout' for join process or " +
                        "'netTimeout' for reconnection) [joinTimeout=2000, netTimeout=5000]").build());

        listeners.add(LogListener.matches(
                "Topology snapshot [ver=1").build());

        listeners.forEach(listeningLog::registerListener);

        startGrid(cfgSrv);

        listeners.forEach(lsnr -> assertTrue(lsnr.check()));
    }

    /**
     * Tests shared IP finder with empty list allows server to start.
     */
    @Test
    public void testServerNodeStartupWithEmptySharedIpFinder() throws Exception {
        dynamicIpFinder.setAddresses(null);

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server", false);

        IgniteEx grid = startGrid(cfgSrv);

        assertEquals(1, grid.cluster().topologyVersion());
    }

    /**
     * Tests non shared IP finder with empty list is not allowed.
     */
    @Test
    public void testServerNodeDynamicIpFinderWithEmptyAddresses() throws Exception {
        dynamicIpFinder.setShared(false);
        dynamicIpFinder.setAddresses(null);

        setRootLoggerDebugLevel();

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false);

        boolean isSpiExThrown = false;

        try {
            startGrid(cfgSrv);

            fail("Server node must fail if non-shared ip finder returns empty list");
        }
        catch (IgniteCheckedException e) {
            if (e.getCause() != null && e.getCause() instanceof IgniteCheckedException) {
                Throwable cause = e.getCause();
                if (cause.getCause() != null && cause.getCause() instanceof IgniteSpiException)
                    isSpiExThrown = true;
            }
        }

        assertTrue("Server node must fail if nonshared dynamic service returns empty list", isSpiExThrown);
    }

    /**
     * Gets node configuration with dynamic IP finder.
     */
    private IgniteConfiguration getConfigurationDynamicIpFinder(String instanceName, boolean clientMode) throws Exception {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(dynamicIpFinder);

        return getConfigurationDynamicIpFinder(instanceName, clientMode, discoverySpi);
    }

    /**
     * Gets node configuration with dynamic IP finder.
     */
    private IgniteConfiguration getConfigurationDynamicIpFinder(String instanceName, boolean clientMode, int joinTimeout) throws Exception {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setJoinTimeout(joinTimeout);
        discoverySpi.setNetworkTimeout(5000);
        discoverySpi.setIpFinder(dynamicIpFinder);

        return getConfigurationDynamicIpFinder(instanceName, clientMode, discoverySpi);
    }

    /**
     * Gets node configuration with dynamic IP finder.
     */
    private IgniteConfiguration getConfigurationDynamicIpFinder(
        String instanceName,
        boolean clientMode,
        TcpDiscoverySpi discoverySpi
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration();
        cfg.setNodeId(null);
        cfg.setLocalHost("127.0.0.1");

        cfg.setDiscoverySpi(discoverySpi);

        cfg.setGridLogger(listeningLog);

        cfg.setIgniteInstanceName(instanceName);
        cfg.setClientMode(clientMode);

        return cfg;
    }
}
