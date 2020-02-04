package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

public class CacheGroupsMetricsPartitionsEvictionBeforeRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_1 = "cache_1";

    /** */
    private static final String CACHE_2 = "cache_2";

    /** */
    private static final String CACHE_3 = "cache_3";

    /** */
    private static final String CACHE_4 = "cache_4";

    /** */
    private static final String GROUP_1 = "group_1";

    /** */
    private static final String GROUP_2 = "group_2";

    /** */
    private static final Random RANDOM = new Random();

    /** */
    private static final int REBALANCE_BATCH_SIZE = 50 * 1024;

    /** Number of loaded keys in each cache. */
    private static final int KEYS_SIZE = 1000;

    private static final int PARTITION_COUNT = 64;


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new RebalanceBlockingSPI());

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setRebalanceThreadPoolSize(4);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxTimeout(1000));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(100L * 1024 * 1024)));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testStopCachesOnDeactivationFirstGroup() throws Exception {
        String groupName = GROUP_1;

        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

        stopGrid(1);

        loadData(ig0);

        IgniteEx ig1 = startGrid(1);

        RebalanceBlockingSPI commSpi = (RebalanceBlockingSPI)ig1.configuration().getCommunicationSpi();

        // Complete all futures for groups that we don't need to wait.
        commSpi.suspendedMessages.forEach((k, v) -> {
            if (k != CU.cacheId(groupName))
                commSpi.resume(k);
        });

        CountDownLatch latch = commSpi.suspendRebalanceInMiddleLatch.get(CU.cacheId(groupName));

        assert latch != null;

        // Await some middle point rebalance for group.
        latch.await();

        ig0.cluster().active(false);
        // Add to escape possible long waiting in awaitPartitionMapExchange due to {@link CacheAffinityChangeMessage}.
        CountDownLatch lastPart = new CountDownLatch(1);
        CountDownLatch getMetric = new CountDownLatch(1);

        subscribeEvictionQueueAtLatch(ig1, getMetric, lastPart);

        ig0.cluster().active(true);


        // Resume rebalance after action performed.
        commSpi.resume(CU.cacheId(groupName));

        LongMetric evictedPartitionsLeft = ig1.context().metric().registry(metricName(CACHE_GROUP_METRICS_PREFIX, GROUP_2))
            .findMetric("RebalancingEvictedPartitionsLeft");

        U.await(lastPart);

        assertEquals(PARTITION_COUNT, evictedPartitionsLeft.value());

        getMetric.countDown();

        awaitPartitionMapExchange();

        assertEquals(0, evictedPartitionsLeft.value());

        //assertNull(grid(1).context().failure().failureContext());
    }

    /**
     * @param ig Ig.
     */
    private void loadData(Ignite ig) {
        List<CacheConfiguration> configs = Stream.of(
            F.t(CACHE_1, GROUP_1),
            F.t(CACHE_2, GROUP_1),
            F.t(CACHE_3, GROUP_2),
            F.t(CACHE_4, GROUP_2)
        ).map(names -> new CacheConfiguration<>(names.get1())
            .setGroupName(names.get2())
            .setRebalanceBatchSize(REBALANCE_BATCH_SIZE)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITION_COUNT))
        ).collect(Collectors.toList());

        ig.getOrCreateCaches(configs);


        configs.forEach(cfg -> {
            try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(cfg.getName())) {
                for (int i = 0; i < KEYS_SIZE; i++)
                    streamer.addData(i, randomArray(128));

                streamer.flush();
            }
        });
    }


    public byte[] randomArray(int size) {
        byte[] arr = new byte[size];

        RANDOM.nextBytes(arr);

        return arr;
    }

    /**
     *
     */
    private static class RebalanceBlockingSPI extends TcpCommunicationSpi {
        /** */
        private final Map<Integer, Queue<T3<UUID, Message, IgniteRunnable>>> suspendedMessages = new ConcurrentHashMap<>();

        /** */
        private final Map<Integer, CountDownLatch> suspendRebalanceInMiddleLatch = new ConcurrentHashMap<>();

        /** */
        RebalanceBlockingSPI() {
            suspendedMessages.put(CU.cacheId(GROUP_1), new ConcurrentLinkedQueue<>());
            suspendedMessages.put(CU.cacheId(GROUP_2), new ConcurrentLinkedQueue<>());
            suspendRebalanceInMiddleLatch.put(CU.cacheId(GROUP_1), new CountDownLatch(3));
            suspendRebalanceInMiddleLatch.put(CU.cacheId(GROUP_2), new CountDownLatch(3));
        }

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
            if (msg instanceof GridIoMessage &&
                ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message();

                CountDownLatch latch = suspendRebalanceInMiddleLatch.get(msg0.groupId());

                if (latch != null) {
                    if (latch.getCount() > 0)
                        latch.countDown();
                    else {
                        synchronized (this) {
                            // Order make sense!
                            Queue<T3<UUID, Message, IgniteRunnable>> q = suspendedMessages.get(msg0.groupId());

                            if (q != null) {
                                q.add(new T3<>(sndId, msg, msgC));

                                return;
                            }
                        }
                    }
                }
            }

            super.notifyListener(sndId, msg, msgC);
        }

        /**
         * @param cacheId Cache id.
         */
        public synchronized void resume(int cacheId){
            for (T3<UUID, Message, IgniteRunnable> t : suspendedMessages.remove(cacheId))
                super.notifyListener(t.get1(), t.get2(), t.get3());
        }
    }

    static class TestQueue extends PriorityBlockingQueue<PartitionsEvictManager.PartitionEvictionTask> {
        private CountDownLatch getMetric;
        private CountDownLatch lastPart;

        public TestQueue(CountDownLatch getMetric, CountDownLatch lastPart) {
            super(1000, Comparator.comparingLong(p -> (U.<GridDhtLocalPartition>field(p, "part").fullSize())));

            this.getMetric = getMetric;
            this.lastPart = lastPart;
        }

        @Override public PartitionsEvictManager.PartitionEvictionTask poll() {
            return getMetric.getCount() == 0 ? super.poll() : null;
        }

        @Override public boolean offer(PartitionsEvictManager.PartitionEvictionTask task) {
            if (U.<GridDhtLocalPartition>field(task, "part").id() == PARTITION_COUNT - 1) {
                lastPart.countDown();

                U.awaitQuiet(getMetric);
            }

            return super.offer(task);
        }

        @Override public int size() {
            return getMetric.getCount() == 0 ? super.size() : 0;
        }
    }

    /**
     * @param node Node.
     */
    protected void subscribeEvictionQueueAtLatch(IgniteEx node, CountDownLatch getMetric, CountDownLatch lastPart) {
        PartitionsEvictManager.BucketQueue evictionQueue = node.context().cache().context().evict().evictionQueue;
        Queue[] buckets = evictionQueue.buckets;

        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new TestQueue(getMetric, lastPart);
    }
}
