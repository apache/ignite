package org.apache.ignite.internal;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class PartitionMapExchangeUnresponsiveNodeTimeoutTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    public void testNonCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        final ClusterNode failNode = ignite(1).cluster().localNode();

        ((TestCommunicationSpi) ignite(1).configuration().getCommunicationSpi()).block = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    public void testCoordinatorUnresponsive() throws Exception {
        startGrids(4);

        final Ignite ig = grid(1);

        ig.cluster().active(true);

        final ClusterNode failNode = ignite(0).cluster().localNode();

        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).block = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    private static class TestCommunicationSpi extends TcpCommunicationSpi {

        private volatile boolean block;

        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

            Message msg0 = ((GridIoMessage)msg).message();
            if ((msg0 instanceof GridDhtPartitionsSingleMessage || msg0 instanceof GridDhtPartitionsFullMessage) && block)
                return;

            super.sendMessage(node, msg, ackC);
        }
    }

}
