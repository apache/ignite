package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;

import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridCacheRebalancingMessageErrorSelfTest extends GridCommonAbstractTest {
    /**
     * partitioned cache name.
     */
    protected static String CACHE = "cache";

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);
        iCfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setName(CACHE);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cfg.setBackups(0);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        iCfg.setCacheConfiguration(cfg);

        return iCfg;
    }

    /**
     * @throws Exception e.
     */
    @Test
    public void test() throws Exception {
        TestRecordingCommunicationSpi sender = TestRecordingCommunicationSpi.spi(startGrid(0));

        for (int i = 0; i < 100; i++)
            grid(0).cache(CACHE).put(i, i);

        sender.closure(new IgniteBiInClosure<ClusterNode, Message>() {
            @Override public void apply(ClusterNode node, Message msg) {

                if (msg instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage message = (GridDhtPartitionSupplyMessage) msg;
                    GridDhtPartitionSupplyMessageV2 errMsg = new GridDhtPartitionSupplyMessageV2(
                        1,
                        message.groupId(),
                        message.topologyVersion(),
                        false,
                        new RuntimeException("Error while supply message")
                    );
                    msg = errMsg;
                }
            }
        });

        startGrid(1);

        for (int i = 0; i < 50; i++)
            assert grid(1).cache(CACHE).get(i) != null;

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}
