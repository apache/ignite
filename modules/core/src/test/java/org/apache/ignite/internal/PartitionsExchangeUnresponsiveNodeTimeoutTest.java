package org.apache.ignite.internal;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeFinishedCheckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeFinishedCheckResponse;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PartitionsExchangeUnresponsiveNodeTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());
        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setExchangeHardTimeout(10_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite ig : G.allGrids()) {
            ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).blockExchangeMessages = false;
            ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).sndOutdatedCheckMessages = false;
        }

        List<Ignite> nodes = G.allGrids();

        while (!nodes.isEmpty()) {
            for (Ignite node : nodes)
                stopGrid(node.name(), true, true);

            nodes = G.allGrids();
        }

        super.afterTest();
    }

    /**
     *
     */
    public void testNonCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        final ClusterNode failNode = ignite(1).cluster().localNode();

        ((TestCommunicationSpi) ignite(1).configuration().getCommunicationSpi()).blockExchangeMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    /**
     *
     */
    public void testCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).blockExchangeMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return ig.cluster().nodes().size() == 1;
            }
        }, 3 * 60_000));
    }

    /**
     *
     */
    public void testCoordinatorUnresponsiveWithCheckMessagesBlocked() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).blockExchangeMessages = true;
        ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).blockCheckMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return ig.cluster().nodes().size() == 1;
            }
        }, 3 * 60_000));
    }

    /**
     *
     */
    public void testCoordinatorUnresponsiveWithOutdatedCheckMessages() throws Exception {
        startGrids(4);

        final Ignite ig = grid(1);

        ig.cluster().active(true);

        UUID failNode = ignite(0).cluster().localNode().id();

        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).blockExchangeMessages = true;
        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).sndOutdatedCheckMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Block exchange messages. */
        private volatile boolean blockExchangeMessages;

        /** Block check messages. */
        private volatile boolean blockCheckMessages;

        /** Send outdated check messages. */
        private volatile boolean sndOutdatedCheckMessages;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

            Message msg0 = ((GridIoMessage)msg).message();
            if ((msg0 instanceof GridDhtPartitionsSingleMessage || msg0 instanceof GridDhtPartitionsFullMessage)
                && blockExchangeMessages)
                return;

            if ((msg0 instanceof PartitionsExchangeFinishedCheckRequest ||
                msg0 instanceof PartitionsExchangeFinishedCheckResponse)
                && blockCheckMessages)
                return;

            if (msg0 instanceof PartitionsExchangeFinishedCheckResponse && sndOutdatedCheckMessages)
                GridTestUtils.setFieldValue(msg0, "topVer", AffinityTopologyVersion.ZERO);

            super.sendMessage(node, msg, ackC);
        }
    }
}
