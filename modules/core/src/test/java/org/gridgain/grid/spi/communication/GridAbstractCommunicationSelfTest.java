/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.communication;

import mx4j.tools.adaptor.http.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.spi.*;

import javax.management.*;
import java.util.*;
import java.util.Map.*;

/**
 * Super class for all communication self tests.
 * @param <T> Type of communication SPI.
 */
@SuppressWarnings({"JUnitAbstractTestClassNamingConvention"})
public abstract class GridAbstractCommunicationSelfTest<T extends GridCommunicationSpi> extends GridSpiAbstractTest<T> {
    /** */
    private static long msgId = 1;

    /** */
    private static final Collection<GridTestResources> spiRsrcs = new ArrayList<>();

    /** */
    private static final Map<UUID, Set<UUID>> msgDestMap = new HashMap<>();

    /** */
    protected static final Map<UUID, GridCommunicationSpi<GridTcpCommunicationMessageAdapter>> spis = new HashMap<>();

    /** */
    protected static final Collection<GridNode> nodes = new ArrayList<>();

    /** */
    private static final Object mux = new Object();

    /** */
    private static final ObjectName mBeanName;

    static {
        GridTcpCommunicationMessageFactory.registerCustom(new GridTcpCommunicationMessageProducer() {
            @Override public GridTcpCommunicationMessageAdapter create(byte type) {
                return new GridTestMessage();
            }
        }, GridTestMessage.DIRECT_TYPE);

        try {
            mBeanName = new ObjectName("mbeanAdaptor:protocol=HTTP");
        }
        catch (MalformedObjectNameException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** */
    @SuppressWarnings({"deprecation"})
    private class MessageListener implements GridCommunicationListener<GridTcpCommunicationMessageAdapter> {
        /** */
        private final UUID locNodeId;

        /**
         * @param locNodeId Local node ID.
         */
        MessageListener(UUID locNodeId) {
            assert locNodeId != null;

            this.locNodeId = locNodeId;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, GridTcpCommunicationMessageAdapter msg, GridRunnable msgC) {
            info("Received message [locNodeId=" + locNodeId + ", nodeId=" + nodeId +
                ", msg=" + msg + ']');

            msgC.run();

            if (msg instanceof GridTestMessage) {
                GridTestMessage testMsg = (GridTestMessage)msg;

                if (!testMsg.getSourceNodeId().equals(nodeId)) {
                    fail("Listener nodeId not equals to message nodeId.");
                }

                synchronized (mux) {
                    // Get list of all recipients for the message.
                    Set<UUID> recipients = msgDestMap.get(testMsg.getSourceNodeId());

                    if (recipients != null) {
                        // Remove this node from a list of recipients.
                        if (!recipients.remove(locNodeId)) {
                            fail("Received unknown message [locNodeId=" + locNodeId + ", msg=" + testMsg + ']');
                        }

                        // If all recipients received their messages,
                        // remove source nodes from sent messages map.
                        if (recipients.isEmpty()) {
                            msgDestMap.remove(testMsg.getSourceNodeId());
                        }

                        if (msgDestMap.isEmpty()) {
                            mux.notifyAll();
                        }
                    }
                    else {
                        fail("Received unknown message [locNodeId=" + locNodeId + ", msg=" + testMsg + ']');
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }

    /** */
    protected GridAbstractCommunicationSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendToOneNode() throws Exception {
        info(">>> Starting send to one node test. <<<");

        msgDestMap.clear();

        for (Entry<UUID, GridCommunicationSpi<GridTcpCommunicationMessageAdapter>> entry : spis.entrySet()) {
            for (GridNode node : nodes) {
                synchronized (mux) {
                    if (!msgDestMap.containsKey(entry.getKey())) {
                        msgDestMap.put(entry.getKey(), new HashSet<UUID>());
                    }

                    msgDestMap.get(entry.getKey()).add(node.id());
                }

                entry.getValue().sendMessage(node, new GridTestMessage(entry.getKey(), msgId++, 0));
            }
        }

        long now = System.currentTimeMillis();
        long endTime = now + getMaxTransmitMessagesTime();

        synchronized (mux) {
            while (now < endTime && !msgDestMap.isEmpty()) {
                mux.wait(endTime - now);

                now = System.currentTimeMillis();
            }

            if (!msgDestMap.isEmpty()) {
                for (Entry<UUID, Set<UUID>> entry : msgDestMap.entrySet()) {
                    error("Failed to receive all messages [sender=" + entry.getKey() +
                        ", dest=" + entry.getValue() + ']');
                }
            }

            assert msgDestMap.isEmpty() : "Some messages were not received.";
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("WaitWithoutCorrespondingNotify")
    public void testSendToManyNodes() throws Exception {
        msgDestMap.clear();

        // Send message from each SPI to all SPI's, including itself.
        for (Entry<UUID, GridCommunicationSpi<GridTcpCommunicationMessageAdapter>> entry : spis.entrySet()) {
            UUID sndId = entry.getKey();

            GridCommunicationSpi<GridTcpCommunicationMessageAdapter> commSpi = entry.getValue();

            for (GridNode node : nodes) {
                synchronized (mux) {
                    if (!msgDestMap.containsKey(sndId)) {
                        msgDestMap.put(sndId, new HashSet<UUID>());
                    }

                    msgDestMap.get(sndId).add(node.id());
                }

                commSpi.sendMessage(node, new GridTestMessage(sndId, msgId++, 0));
            }
        }

        long now = System.currentTimeMillis();
        long endTime = now + getMaxTransmitMessagesTime();

        synchronized (mux) {
            while (now < endTime && !msgDestMap.isEmpty()) {
                mux.wait(endTime - now);

                now = System.currentTimeMillis();
            }

            if (!msgDestMap.isEmpty()) {
                for (Entry<UUID, Set<UUID>> entry : msgDestMap.entrySet()) {
                    error("Failed to receive all messages [sender=" + entry.getKey() +
                        ", dest=" + entry.getValue() + ']');
                }
            }

            assert msgDestMap.isEmpty() : "Some messages were not received.";
        }
    }

    /**
     * @param idx Node index.
     * @return Spi.
     */
    protected abstract GridCommunicationSpi<GridTcpCommunicationMessageAdapter> getSpi(int idx);

    /**
     * @return Spi count.
     */
    protected int getSpiCount() {
        return 2;
    }

    /**
     * @return Max time for message delivery.
     */
    protected int getMaxTransmitMessagesTime() {
        return 20000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();

        Map<GridNode, GridSpiTestContext> ctxs = new HashMap<>();

        for (int i = 0; i < getSpiCount(); i++) {
            GridCommunicationSpi<GridTcpCommunicationMessageAdapter> spi = getSpi(i);

            GridTestResources rsrcs = new GridTestResources(getMBeanServer(i));

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            GridSpiTestContext ctx = initSpiContext();

            ctx.setLocalNode(node);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            spi.setListener(new MessageListener(rsrcs.getNodeId()));

            node.setAttributes(spi.getNodeAttributes());

            nodes.add(node);

            spi.spiStart(getTestGridName() + (i + 1));

            spis.put(rsrcs.getNodeId(), spi);

            spi.onContextInitialized(ctx);

            ctxs.put(node, ctx);
        }

        // For each context set remote nodes.
        for (Entry<GridNode, GridSpiTestContext> e : ctxs.entrySet()) {
            for (GridNode n : nodes) {
                if (!n.equals(e.getKey()))
                    e.getValue().remoteNodes().add(n);
            }
        }
    }

    /**
     * @param idx Node index.
     * @return Configured MBean server.
     * @throws Exception If failed.
     */
    private MBeanServer getMBeanServer(int idx) throws Exception {
        HttpAdaptor mbeanAdaptor = new HttpAdaptor();

        MBeanServer mbeanSrv = MBeanServerFactory.createMBeanServer();

        mbeanAdaptor.setPort(
            Integer.valueOf(GridTestProperties.getProperty("comm.mbeanserver.selftest.baseport")) + idx);

        mbeanSrv.registerMBean(mbeanAdaptor, mBeanName);

        mbeanAdaptor.start();

        return mbeanSrv;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (GridCommunicationSpi<GridTcpCommunicationMessageAdapter> spi : spis.values()) {
            spi.setListener(null);

            spi.spiStop();
        }

        for (GridTestResources rsrcs : spiRsrcs) {
            rsrcs.stopThreads();

            rsrcs.getMBeanServer().unregisterMBean(mBeanName);
        }
    }
}
