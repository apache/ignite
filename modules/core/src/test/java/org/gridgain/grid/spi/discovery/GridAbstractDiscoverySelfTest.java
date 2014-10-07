/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery;

import mx4j.tools.adaptor.http.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.spi.*;

import javax.management.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.product.GridProductVersion.*;

/**
 * Base discovery self-test class.
 * @param <T> SPI implementation class.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public abstract class GridAbstractDiscoverySelfTest<T extends GridSpi> extends GridSpiAbstractTest<T> {
    /** */
    private static final List<GridDiscoverySpi> spis = new ArrayList<>();

    /** */
    private static final Collection<GridTestResources> spiRsrcs = new ArrayList<>();

    /** */
    private static long spiStartTime;

    /** */
    private static final Object mux = new Object();

    /** */
    private static final String TEST_ATTRIBUTE_NAME = "test.node.prop";

    /** */
    protected GridAbstractDiscoverySelfTest() {
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
            for (GridDiscoverySpi spi : spis) {
                if (spi.getRemoteNodes().size() < (getSpiCount() - 1)) {
                    isAllDiscovered = false;

                    break;
                }

                isAllDiscovered = true;

                for (GridTestResources rscrs : spiRsrcs) {
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
                        GridDiscoverySpi spi = spis.get(i);

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
    private static class DiscoveryListener implements GridDiscoverySpiListener {
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
        @Override public void onDiscovery(int type, long topVer, GridNode node, Collection<GridNode> topSnapshot,
            Map<Long, Collection<GridNode>> topHist) {
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

        for (GridDiscoverySpi spi : spis) {
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
                        GridDiscoverySpi spi = spis.get(i);

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

        for (final GridDiscoverySpi spi : spis) {
            final AtomicInteger spiCnt = new AtomicInteger(0);

            GridDiscoverySpiListener locHeartbeatLsnr = new GridDiscoverySpiListener() {
                @Override public void onDiscovery(int type, long topVer, GridNode node,
                    Collection<GridNode> topSnapshot, Map<Long, Collection<GridNode>> topHist) {
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
    private boolean isContainsNodeId(Iterable<GridNode> nodes, UUID nodeId) {
        for (GridNode node : nodes) {
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
        for (GridDiscoverySpi spi : spis) {
            GridNode loc = spi.getLocalNode();

            Collection<GridNode> rmt = spi.getRemoteNodes();

            assert !rmt.contains(loc);
        }
    }

    /**
     * Check that "test.node.prop" is present on all nodes.
     */
    public void testNodeAttributes() {
        for (GridDiscoverySpi spi : spis) {
            assert !spi.getRemoteNodes().isEmpty() : "No remote nodes found in Spi.";

            Collection<UUID> nodeIds = new HashSet<>();

            for (GridTestResources rsrc : spiRsrcs) {
                nodeIds.add(rsrc.getNodeId());
            }

            for (GridNode node : spi.getRemoteNodes()) {
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
        for (GridDiscoverySpi spi : spis) {
            for (GridTestResources rscrs : spiRsrcs) {
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
        for (GridDiscoverySpi spi : spis) {
            GridNode node = spi.getLocalNode();

            assert node != null;

            writeObject(node);

            info("Serialize node success [nodeId=" + node.id() + ", spiIdx=" + spis.indexOf(spi) + ']');
        }
    }

    /**
     * @param idx Index.
     * @return Discovery SPI.
     */
    protected abstract GridDiscoverySpi getSpi(int idx);

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
                GridDiscoverySpi spi = getSpi(i);

                GridTestResources rsrcMgr = new GridTestResources(getMBeanServer(i));

                rsrcMgr.inject(spi);

                spi.setNodeAttributes(Collections.<String, Object>singletonMap(TEST_ATTRIBUTE_NAME, "true"),
                    fromString("99.99.99"));

                spi.setListener(new GridDiscoverySpiListener() {
                    @SuppressWarnings({"NakedNotify"})
                    @Override public void onDiscovery(int type, long topVer, GridNode node,
                        Collection<GridNode> topSnapshot, Map<Long, Collection<GridNode>> topHist) {
                        info("Discovery event [type=" + type + ", node=" + node + ']');

                        synchronized (mux) {
                            mux.notifyAll();
                        }
                    }
                });

                spi.setDataExchange(new GridDiscoverySpiDataExchange() {
                    @Override public List<Object> collect(UUID nodeId) {
                        return new LinkedList<>();
                    }

                    @Override public void onExchange(List<Object> data) {
                        // No-op.
                    }
                });

                spi.setAuthenticator(new GridDiscoverySpiNodeAuthenticator() {
                    @Override public GridSecurityContext authenticateNode(GridNode n, GridSecurityCredentials cred) {
                        GridSecuritySubjectAdapter subj = new GridSecuritySubjectAdapter(
                            GridSecuritySubjectType.REMOTE_NODE, n.id());

                        subj.permissions(new GridAllowAllPermissionSet());

                        return new GridSecurityContext(subj);
                    }
                });


                spi.spiStart(getTestGridName() + i);

                spis.add(spi);

                spiRsrcs.add(rsrcMgr);

                // Force to use test context instead of default dummy context.
                spi.onContextInitialized(initSpiContext());
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

        srv.registerMBean(adaptor, new ObjectName("mbeanAdaptor:protocol=HTTP"));

        adaptor.start();

        return srv;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        assert spis.size() > 1;
        assert spis.size() == spiRsrcs.size();

        for (GridDiscoverySpi spi : spis) {
            spi.setListener(null);

            spi.spiStop();
        }

        for (GridTestResources rscrs : spiRsrcs) {
            rscrs.stopThreads();
        }

        // Clear.
        spis.clear();
        spiRsrcs.clear();

        spiStartTime = 0;

        tearDown();
    }

    /**
     * @param node Grid node.
     * @throws IOException If write failed.
     */
    private void writeObject(GridNode node) throws Exception {

        GridMarshaller marshaller = getTestResources().getMarshaller();

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
