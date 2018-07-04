package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

public class PendingMessageDeliveryTest extends GridCommonAbstractTest {
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private AtomicBoolean blockMsgs;

    private Set<IgniteUuid> receivedCustomMsgs;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        blockMsgs = new AtomicBoolean(false);
        receivedCustomMsgs = new ConcurrentHashSet<>();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco;

        if (igniteInstanceName.startsWith("victim"))
            disco = new DyingDiscoverySpi();
        else if (igniteInstanceName.startsWith("listener"))
            disco = new ListeningDiscoverySpi();
        else
            disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPendingMessagesOverflow() throws Exception {
        Ignite coord = startGrid("coordinator");
        TcpDiscoverySpi coordDisco = (TcpDiscoverySpi)coord.configuration().getDiscoverySpi();

        // Victim doesn't send acknowledges, so we need an intermediate node to accept messages,
        // so the coordinator could mark them as pending.
        Ignite mediator = startGrid("mediator");

        Ignite victim = startGrid("victim");

        startGrid("listener");

        // Initial custom message will travel across the ring and will be discarded.
        sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                log.info("Waiting for initial custom message: " + receivedCustomMsgs.size());

                return receivedCustomMsgs.size() == 1;
            }
        }, 10000));

        receivedCustomMsgs.clear();

        blockMsgs.set(true);

        log.info("Sending dummy custom messages");

        // Non-discarded messages shouldn't be dropped from the queue.
        int msgsNum = 2000;

        for (int i = 0; i < msgsNum; i++)
            sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        mediator.close();
        victim.close();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                log.info("Received custom messages: " + receivedCustomMsgs.size());

                return receivedCustomMsgs.size() == msgsNum;
            }
        }, 10000));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomMessageInSingletonCluster() throws Exception {
        Ignite coord = startGrid("coordinator");
        TcpDiscoverySpi coordDisco = (TcpDiscoverySpi)coord.configuration().getDiscoverySpi();

        // Custom message on a singleton cluster shouldn't break consistency of PendingMessages.
        sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        // Victim doesn't send acknowledges, so we need an intermediate node to accept messages,
        // so the coordinator could mark them as pending.
        Ignite mediator = startGrid("mediator");

        Ignite victim = startGrid("victim");

        startGrid("listener");

        assertTrue(receivedCustomMsgs.isEmpty());

        blockMsgs.set(true);

        log.info("Sending dummy custom messages");

        // Non-discarded messages shouldn't be dropped from the queue.
        int msgsNum = 100;

        for (int i = 0; i < msgsNum; i++)
            sendDummyCustomMessage(coordDisco, IgniteUuid.randomUuid());

        mediator.close();
        victim.close();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                log.info("Received custom messages: " + receivedCustomMsgs.size());

                return receivedCustomMsgs.size() == msgsNum;
            }
        }, 10000));
    }

    private void sendDummyCustomMessage(TcpDiscoverySpi disco, IgniteUuid id) {
        disco.sendCustomEvent(new CustomMessageWrapper(new DummyCustomDiscoveryMessage(id)));
    }

    private class DyingDiscoverySpi extends TcpDiscoverySpi {
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (!blockMsgs.get())
                super.writeToSocket(sock, msg, data, timeout);
        }

        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (!blockMsgs.get())
                super.writeToSocket(sock, msg, timeout);
        }

        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (!blockMsgs.get())
                super.writeToSocket(sock, out, msg, timeout);
        }

        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (!blockMsgs.get())
                super.writeToSocket(msg, sock, res, timeout);
        }
    }

    private class ListeningDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryCustomEventMessage) {
                TcpDiscoveryCustomEventMessage tcpCustomMsg = (TcpDiscoveryCustomEventMessage)msg;
                try {
                    DiscoverySpiCustomMessage customMsg = tcpCustomMsg.message(marshaller(), U.gridClassLoader());
                    DiscoveryCustomMessage delegate = ((CustomMessageWrapper)customMsg).delegate();

                    if (delegate instanceof DummyCustomDiscoveryMessage)
                        receivedCustomMsgs.add(delegate.id());
                }
                catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }
    }

    private static class DummyCustomDiscoveryMessage implements DiscoveryCustomMessage {
        private final IgniteUuid id;

        public DummyCustomDiscoveryMessage(IgniteUuid id) {
            this.id = id;
        }

        @Override public IgniteUuid id() {
            return id;
        }

        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return null;
        }

        @Override public boolean isMutable() {
            return false;
        }

        @Override public boolean stopProcess() {
            return false;
        }

        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }
    }
}

