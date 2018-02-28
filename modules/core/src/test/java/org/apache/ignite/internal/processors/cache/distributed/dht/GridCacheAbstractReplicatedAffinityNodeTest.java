package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Abstract test to check for new
 * {@link org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture#affinityNode(java.util.List) affinityNode}
 * method for replicated and partitioned caches
 */
public abstract class GridCacheAbstractReplicatedAffinityNodeTest extends GridCommonAbstractTest {
    private static final int KEY = 1;
    private static final int NODE_COUNT = 4;

    @Override
    protected void beforeTestsStarted() throws Exception {
        startGrids(NODE_COUNT);
    }

    @Override
    protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);
        configuration.setCommunicationSpi(new CheckNodeCommunicationSpi());

        if (igniteInstanceName.equals(getTestIgniteInstanceName(NODE_COUNT - 1))) {
            configuration.setClientMode(true);
        } else {
            CacheConfiguration cacheConfiguration = new CacheConfiguration(DEFAULT_CACHE_NAME);
            cacheConfiguration.setCacheMode(CacheMode.REPLICATED);
            cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheConfiguration.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheConfiguration.setReadFromBackup(readFromBackup());

            configuration.setCacheConfiguration(cacheConfiguration);
        }

        return configuration;
    }

    protected int getKey() {
        return KEY;
    }

    protected Ignite getClientNode() {
        return ignite(NODE_COUNT - 1);
    }

    protected CheckNodeCommunicationSpi communication(Ignite ignite) {
        return (CheckNodeCommunicationSpi) ignite.configuration().getCommunicationSpi();
    }

    protected abstract boolean readFromBackup();

    /**
     * Custom TcpCommunicationSpi to check for expected node
     */
    protected static class CheckNodeCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile ClusterNode expectedNode;

        /**
         * @param expectedNode expected node
         */
        public void setExpectedNode(final ClusterNode expectedNode) {
            this.expectedNode = expectedNode;
        }

        @Override
        public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                final Message realMessage = ((GridIoMessage) msg).message();
                if (realMessage instanceof GridNearSingleGetRequest) {
                    Assert.assertEquals(expectedNode, node);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
