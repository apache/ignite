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

package org.apache.ignite.spi.discovery;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import mx4j.tools.adaptor.http.HttpAdaptor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.lang.IgniteProductVersion.fromString;

/**
 * Base discovery self-test class.
 * @param <T> SPI implementation class.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public abstract class AbstractDiscoverySelfTest<T extends IgniteSpi> extends GridSpiAbstractTest<T> {
    /** */
    private static final String HTTP_ADAPTOR_MBEAN_NAME = "mbeanAdaptor:protocol=HTTP";

    /** */
    protected static final List<DiscoverySpi> spis = new ArrayList<>();

    /** */
    private static final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** */
    private static final List<HttpAdaptor> httpAdaptors = new ArrayList<>();

    /** */
    private static long spiStartTime;

    /** */
    private static final Object mux = new Object();

    /** */
    private static final String TEST_ATTRIBUTE_NAME = "test.node.prop";

    /** */
    protected boolean useSsl = false;

    /** */
    protected AbstractDiscoverySelfTest() {
        super(false);
    }

    /**
     * Checks that each started discovery spi found all other SPI's.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnconditionalWait"})
    public void testDiscovery() throws Exception {
        assert spis.size() > 1;
        assert spiStartTime > 0;
        assert spiRsrcs.size() == getSpiCount();

        boolean isAllDiscovered = false;

        while (!isAllDiscovered) {
            for (DiscoverySpi spi : spis) {
                if (spi.getRemoteNodes().size() < (getSpiCount() - 1)) {
                    isAllDiscovered = false;

                    break;
                }

                isAllDiscovered = true;

                for (IgniteTestResources rscrs : spiRsrcs) {
                    UUID nodeId = rscrs.getNodeId();

                    if (!nodeId.equals(spi.getLocalNode().id())) {
                        if (!isContainsNodeId(spi.getRemoteNodes(), nodeId)) {
                            isAllDiscovered = false;

                            break;
                        }
                    }
                }
            }

            if (isAllDiscovered)
                info("All nodes discovered.");
            else {
                if (System.currentTimeMillis() > spiStartTime + getMaxDiscoveryTime()) {
                    for (int i = 0; i < getSpiCount(); i++) {
                        DiscoverySpi spi = spis.get(i);

                        info("Remote nodes [spiIdx=" + i + ", nodes=" + spi.getRemoteNodes() + ']');
                    }

                    fail("Nodes were not discovered.");
                }
                else {
                    synchronized (mux) {
                        mux.wait(getMaxDiscoveryTime());
                    }
                }
            }
        }
    }

    /** */
    private static class DiscoveryListener implements DiscoverySpiListener {
        /** * */
        private boolean isMetricsUpdate;

        /**
         *
         *
         * @return Metrics updates.
         */
        public boolean isMetricsUpdated() {
            return isMetricsUpdate;
        }

        /** {@inheritDoc} */
        @Override public void onDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> topSnapshot,
            Map<Long, Collection<ClusterNode>> topHist, @Nullable DiscoverySpiCustomMessage data) {
            if (type == EVT_NODE_METRICS_UPDATED)
                isMetricsUpdate = true;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnconditionalWait"})
    public void testMetrics() throws Exception {
        Collection<DiscoveryListener> listeners = new ArrayList<>();

        long metricsStartTime = System.currentTimeMillis();

        for (DiscoverySpi spi : spis) {
            DiscoveryListener metricsUpdateLsnr = new DiscoveryListener();

            spi.setListener(metricsUpdateLsnr);

            listeners.add(metricsUpdateLsnr);
        }

        boolean isAllSpiMetricUpdated = false;

        while (!isAllSpiMetricUpdated) {
            isAllSpiMetricUpdated = true;

            for (DiscoveryListener lsnr : listeners) {
                if (!lsnr.isMetricsUpdated()) {
                    isAllSpiMetricUpdated = false;

                    break;
                }
            }

            if (isAllSpiMetricUpdated)
                info("All SPI metrics updated.");
            else {
                if (System.currentTimeMillis() > metricsStartTime + getMaxMetricsWaitTime()) {
                    for (int i = 0; i < getSpiCount(); i++) {
                        DiscoverySpi spi = spis.get(i);

                        info("Remote nodes [spiIdx=" + i + ", nodes=" + spi.getRemoteNodes() + ']');
                    }

                    fail("SPI Metrics not updated.");
                }
                else {
                    synchronized (mux) {
                        mux.wait(getMaxMetricsWaitTime());
                    }
                }
            }
        }
    }

    /**
     * Tests whether local node heartbeats cause METRICS_UPDATE event.
     *
     * @throws Exception If test failed.
     */
    public void testLocalHeartbeat() throws Exception {
        AtomicInteger[] locUpdCnts = new AtomicInteger[getSpiCount()];

        int i = 0;

        for (final DiscoverySpi spi : spis) {
            final AtomicInteger spiCnt = new AtomicInteger(0);

            DiscoverySpiListener locHeartbeatLsnr = new DiscoverySpiListener() {
                @Override public void onDiscovery(int type, long topVer, ClusterNode node,
                    Collection<ClusterNode> topSnapshot, Map<Long, Collection<ClusterNode>> topHist,
                    @Nullable DiscoverySpiCustomMessage data) {
                    // If METRICS_UPDATED came from local node
                    if (type == EVT_NODE_METRICS_UPDATED
                        && node.id().equals(spi.getLocalNode().id()))
                        spiCnt.addAndGet(1);
                }
            };

            locUpdCnts[i] = spiCnt;

            spi.setListener(locHeartbeatLsnr);

            i++;
        }

        // Sleep fro 3 Heartbeats.
        Thread.sleep(getMaxDiscoveryTime() * 3);

        for (AtomicInteger cnt : locUpdCnts) {
            assert cnt.get() > 1 : "One of the SPIs did not get at least 2 METRICS_UPDATE events from local node";
        }
    }

    /**
     * @param nodes Nodes iterator.
     * @param nodeId Node UUID.
     * @return {@code true} if provided iterator contains node with provided UUID.
     */
    private boolean isContainsNodeId(Iterable<ClusterNode> nodes, UUID nodeId) {
        for (ClusterNode node : nodes) {
            assert node.id() != null;

            if (node.id().equals(nodeId))
                return true;
        }

        return false;
    }

    /**
     * Checks that physical address of local node is equal to local.ip property.
     */
    public void testLocalNode() {
        for (DiscoverySpi spi : spis) {
            ClusterNode loc = spi.getLocalNode();

            Collection<ClusterNode> rmt = spi.getRemoteNodes();

            assert !rmt.contains(loc);
        }
    }

    /**
     * Check that "test.node.prop" is present on all nodes.
     */
    public void testNodeAttributes() {
        for (DiscoverySpi spi : spis) {
            assert !spi.getRemoteNodes().isEmpty() : "No remote nodes found in Spi.";

            Collection<UUID> nodeIds = new HashSet<>();

            for (IgniteTestResources rsrc : spiRsrcs)
                nodeIds.add(rsrc.getNodeId());

            for (ClusterNode node : spi.getRemoteNodes()) {
                if (nodeIds.contains(node.id())) {
                    Serializable attr = node.attribute(TEST_ATTRIBUTE_NAME);

                    if (attr == null || !(attr instanceof String)) {
                        fail("Node does not contains attribute [attr=" + TEST_ATTRIBUTE_NAME + ", nodeId=" +
                            node.id() + ", spiIdx=" + spis.indexOf(spi) + ']');
                    }
                    else if (!"true".equals(attr)) {
                        fail("Attribute value is wrong [attr=" + TEST_ATTRIBUTE_NAME + ", value=" + attr + ", nodeId=" +
                            node.id() + ", spiIdx=" + spis.indexOf(spi) + ']');
                    }
                    else {
                        info("Node contains attribute [attr=" + TEST_ATTRIBUTE_NAME + ", value=" + attr + ", nodeId=" +
                            node.id() + ", spiIdx=" + spis.indexOf(spi) + ']');
                    }
                }
                else
                    error("Discovered unknown node [node=" + node + ", spiIdx=" + spis.indexOf(spi) + ']');
            }
        }
    }

    /**
     * Checks that each spi can pings all other.
     */
    public void testPing() {
        for (DiscoverySpi spi : spis) {
            for (IgniteTestResources rscrs : spiRsrcs) {
                UUID nodeId = rscrs.getNodeId();

                if (spi.pingNode(nodeId))
                    info("Ping node success [nodeId=" + nodeId + ", spiIdx=" + spis.indexOf(spi) + ']');
                else
                    fail("Ping node error [nodeId=" + nodeId + ", spiIdx=" + spis.indexOf(spi) + ']');
            }
        }
    }

    /**
     * Checks that node serializable.
     *
     * @throws Exception If failed.
     */
    public void testNodeSerialize() throws Exception {
        for (DiscoverySpi spi : spis) {
            ClusterNode node = spi.getLocalNode();

            assert node != null;

            writeObject(node);

            info("Serialize node success [nodeId=" + node.id() + ", spiIdx=" + spis.indexOf(spi) + ']');
        }
    }

    /**
     * @param idx Index.
     * @return Discovery SPI.
     */
    protected abstract DiscoverySpi getSpi(int idx);

    /**
     * @return SPI count.
     */
    protected int getSpiCount() {
        return 2;
    }

    /**
     * @return Maximum discovery time.
     */
    protected long getMaxDiscoveryTime() {
        return 10000;
    }

    /**
     * @return Maximum metrics wait time.
     */
    protected long getMaxMetricsWaitTime() {
        return getMaxDiscoveryTime();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        try {
            for (int i = 0; i < getSpiCount(); i++) {
                DiscoverySpi spi = getSpi(i);

                IgniteTestResources rsrcMgr = new IgniteTestResources(getMBeanServer(i));

                rsrcMgr.inject(spi);

                spi.setNodeAttributes(Collections.<String, Object>singletonMap(TEST_ATTRIBUTE_NAME, "true"),
                    fromString("99.99.99"));

                spi.setListener(new DiscoverySpiListener() {
                    @SuppressWarnings({"NakedNotify"})
                    @Override public void onDiscovery(int type, long topVer, ClusterNode node,
                        Collection<ClusterNode> topSnapshot, Map<Long, Collection<ClusterNode>> topHist,
                        @Nullable DiscoverySpiCustomMessage data) {
                        info("Discovery event [type=" + type + ", node=" + node + ']');

                        synchronized (mux) {
                            mux.notifyAll();
                        }
                    }
                });

                spi.setDataExchange(new DiscoverySpiDataExchange() {
                    @Override public Map<Integer, Serializable> collect(UUID nodeId) {
                        return new HashMap<>();
                    }

                    @Override public void onExchange(UUID joiningNodeId, UUID nodeId, Map<Integer, Serializable> data) {
                        // No-op.
                    }
                });

                GridSpiTestContext ctx = initSpiContext();

                GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "spiCtx", ctx);

                if (useSsl) {
                    IgniteMock ignite = GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "ignite");

                    IgniteConfiguration cfg = ignite.configuration()
                        .setSslContextFactory(GridTestUtils.sslFactory());

                    ignite.setStaticCfg(cfg);
                }

                spi.spiStart(getTestGridName() + i);

                spis.add(spi);

                spiRsrcs.add(rsrcMgr);

                // Force to use test context instead of default dummy context.
                spi.onContextInitialized(ctx);
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }

        spiStartTime = System.currentTimeMillis();
    }

    /**
     * @param idx Index.
     * @return MBean server.
     * @throws Exception If failed.
     */
    private MBeanServer getMBeanServer(int idx) throws Exception {
        HttpAdaptor adaptor = new HttpAdaptor();

        MBeanServer srv = MBeanServerFactory.createMBeanServer();

        adaptor.setPort(Integer.valueOf(GridTestProperties.getProperty("discovery.mbeanserver.selftest.baseport")) +
            idx);

        srv.registerMBean(adaptor, new ObjectName(HTTP_ADAPTOR_MBEAN_NAME));

        adaptor.start();

        httpAdaptors.add(adaptor);

        return srv;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        assert spis.size() > 1;
        assert spis.size() == spiRsrcs.size();

        for (DiscoverySpi spi : spis) {
            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rscrs : spiRsrcs) {
            MBeanServer mBeanServer = rscrs.getMBeanServer();

            mBeanServer.unregisterMBean(new ObjectName(HTTP_ADAPTOR_MBEAN_NAME));

            rscrs.stopThreads();
        }

        for (HttpAdaptor adaptor : httpAdaptors)
            adaptor.stop();

        // Clear.
        spis.clear();
        spiRsrcs.clear();
        httpAdaptors.clear();

        spiStartTime = 0;

        tearDown();
    }

    /**
     * @param node Grid node.
     * @throws IOException If write failed.
     */
    private void writeObject(ClusterNode node) throws Exception {
        Marshaller marshaller = getTestResources().getMarshaller();

        OutputStream out = new NullOutputStream();

        try {
            marshaller.marshal(node, out);
        }
        finally {
            U.close(out, null);
        }
    }

    /**
     *
     */
    private static class NullOutputStream extends OutputStream {
        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            // No-op
        }
    }
}