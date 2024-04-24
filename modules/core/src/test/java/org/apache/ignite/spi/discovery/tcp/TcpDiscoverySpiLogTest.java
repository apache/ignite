package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for {@link TcpDiscoverySpi} logging tests.
 */
public class TcpDiscoverySpiLogTest extends GridCommonAbstractTest {
    /**
     * Listener log messages
     */
    private static ListeningTestLogger testLog;

    /** */
    private static final ThreadLocal<TcpDiscoverySpi> nodeSpi = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = nodeSpi.get();

        if (spi == null)
            spi = new TcpDiscoverySpi();
        else
            nodeSpi.set(null);

        cfg.setDiscoverySpi(spi);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog.clearListeners();

        super.afterTest();
    }

    /**
     * This test checks all the possible logs from socket connection after coordinator quiet close.
     * <p>
     * There are 3 nodes used in the test. After the coordinator closure, the 3rd node will try to check the connection
     * to the 2nd one. In other words the 3rd will try to connect to all effective addresses of the 2nd through socket,
     * which can be accessed with 3's Discovery SPI.
     * <p>
     * For us to get multiple effective addresses for connection, we override
     * {@link TcpDiscoverySpi#getEffectiveNodeAddresses(TcpDiscoveryNode)} method in
     * {@link TestCustomEffectiveNodeAddressesSpi} to return custom collection of {@link InetSocketAddress}.
     * We only need the custom collection once, when 3rd node is trying to probe the 2nd one. As the method is used in
     * different other scenarios throughout the test, we restrain the method to deviate from expected behaviour only
     * for the test case, that is when {@link TcpDiscoveryNode#discoveryPort()} is 47501.
     * <p>
     * By manipulating the collection order we can check the failing, successful and ignored connection logs at once.
     *
     * @see TcpDiscoverySpi#getEffectiveNodeAddresses(TcpDiscoveryNode)
     * @see TestCustomEffectiveNodeAddressesSpi
     * @throws Exception If failed.
     * */
    @Test
    public void testMultipleSocketConnectionLogMessage() throws Exception {
        LogListener lsnr = LogListener.matches(s ->
            s.contains("Successful connection to the server [address=") ||
            s.contains("Failed to connect the socket to the server [address=") ||
            s.contains("Connection to the server [address=")).build();

        testLog.registerListener(lsnr);

        try {
            startGrid(0);
            startGrid(1);

            TestCustomEffectiveNodeAddressesSpi spi = new TestCustomEffectiveNodeAddressesSpi();
            nodeSpi.set(spi);

            startGrid(2);

            ((TcpDiscoverySpi) ignite(0).configuration().getDiscoverySpi()).brakeConnection();

            assertTrue(waitForCondition(lsnr::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * This test uses quiet closure of given {@link Socket} ignoring possible checked exception, which triggers
     * the previous node to check the connection to the following surviving one.
     * @see TcpDiscoverySpi#brakeConnection()
     * @throws Exception If failed.
     * */
    @Test
    public void testCheckBrakeConnectionSuccessSocketConnectionLogMessage() throws Exception {
        LogListener lsnr = LogListener.matches("Successful connection to the server [address=")
            .atLeast(1)
            .build();

        testLog.registerListener(lsnr);

        try {
            startGridsMultiThreaded(3);
            ((TcpDiscoverySpi) ignite(0).configuration().getDiscoverySpi()).brakeConnection();
            assertTrue(waitForCondition(lsnr::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * This test uses node failure by stopping service threads, which makes the node unresponsive and results in
     * failing connection to the server.
     * @see TcpDiscoverySpi#simulateNodeFailure()
     * @throws Exception If failed.
     * */
    @Test
    public void testCheckNodeFailureSocketConnectionLogMessage() throws Exception {
        LogListener lsnr = LogListener.matches("Failed to connect the socket to the server [address=")
            .atLeast(1)
            .build();

        testLog.registerListener(lsnr);

        try {
            startGridsMultiThreaded(3);
            ((TcpDiscoverySpi) ignite(0).configuration().getDiscoverySpi()).simulateNodeFailure();

            assertTrue(waitForCondition(lsnr::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Simulates coordinator's {@code NODE_FAILED} event
     * <p>
     * Coordinator's (node-0) {@code NODE_FAILED} event is forced on the second in line node in the ring (node-1).
     * This event leads the cluster to fall apart into 2 parts: coordinator and others nodes. As the previously known
     * coordinator node (node-0) is not accessible anymore, the log should contain 'Failed to connect the socket
     * to the server', when trying to check the connection from node-1.
     * @throws Exception If failed.
     */
    @Test
    public void testCheckSocketConnectionFailureLogMessage() throws Exception {
        LogListener lsnr = LogListener.matches("Failed to connect the socket to the server [address=")
            .atLeast(1)
            .build();

        testLog.registerListener(lsnr);

        try {
            IgniteEx coord = (IgniteEx)startGridsMultiThreaded(3);

            UUID coordId = coord.localNode().id();

            IgniteEx ignite1 = grid(1);

            CountDownLatch failedLatch = new CountDownLatch(2);

            IgnitePredicate<Event> failLsnr = evt -> {
                assertEquals(EVT_NODE_FAILED, evt.type());

                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                if (coordId.equals(nodeId))
                    failedLatch.countDown();

                return true;
            };

            ignite1.events().localListen(failLsnr, EVT_NODE_FAILED);

            grid(2).events().localListen(failLsnr, EVT_NODE_FAILED);

            ignite1.configuration().getDiscoverySpi().failNode(coordId, null);

            assertTrue(waitForCondition(lsnr::check, 10_000L));;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Emulates coordinator's {@code NODE_FAILED} event in {@link TcpDiscoveryNodeAddedMessage} while new node joins
     * the cluster with following coordinator shutdown.
     * <p>
     * Coordinator's (node-0) {@code NODE_FAILED} event, happening on {@link TcpDiscoveryNodeAddedMessage} processing
     * with following coordinator's shutdown, prompts the new node (node-2) to check previous node's addresses (node-1).
     * As the previous node (node-1) is already established in new topology after the coordinator (node-0) shutdown,
     * the log should contain 'Success connection to the server' message, which describes successful connection
     * between new node (node-2) and previous surviving node (node-1).
     * @throws Exception If failed.
     */
    @Test
    public void testCheckSocketConnectionSuccessLogMessage() throws Exception {
        LogListener lsnr = LogListener.matches("Successful connection to the server [address=")
            .atLeast(1)
            .build();

        testLog.registerListener(lsnr);

        try {
            TestCustomEventLogSocketSpi spi0 = new TestCustomEventLogSocketSpi();

            nodeSpi.set(spi0);

            final Ignite ignite0 = startGrid(0);

            TcpDiscoverySpi spi1 = new TcpDiscoverySpi();

            nodeSpi.set(spi1);

            startGrid(1);

            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);

            spi0.nodeAdded1 = latch1;
            spi0.nodeAdded2 = latch2;
            spi0.debug = true;

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(2);

                    return null;
                }
            });

            latch1.await();

            spi0.stop = true;

            latch2.countDown();

            ignite0.close();

            fut1.get();

            assertTrue(waitForCondition(lsnr::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    private static class TestCustomEffectiveNodeAddressesSpi extends TcpDiscoverySpi {
        private final static LinkedHashSet<InetSocketAddress> customAddrs = new LinkedHashSet<>();

        static {
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47505));
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47501));
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47503));
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47504));
        }

        /** {@inheritDoc} */
        @Override LinkedHashSet<InetSocketAddress> getEffectiveNodeAddresses(TcpDiscoveryNode node) {
            if(node.discoveryPort() == 47501)
                return customAddrs;

            return super.getEffectiveNodeAddresses(node);
        }
    }

    private static class TestCustomEventLogSocketSpi extends TcpDiscoverySpi {
        /** */
        private volatile CountDownLatch nodeAdded1;

        /** */
        private volatile CountDownLatch nodeAdded2;

        /** */
        private volatile boolean stop;

        /** */
        private boolean debug;

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                if (nodeAdded1 != null) {
                    nodeAdded1.countDown();

                    if (debug)
                        log.info("--- Wait node added: " + msg);

                    U.await(nodeAdded2);

                    if (stop)
                        return;
                }
            }

            super.writeToSocket(sock, out, msg, timeout);
        }
    }
}
