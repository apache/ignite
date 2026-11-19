package org.apache.ignite.cache.affinity;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.EventAdapter;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 */
public class RebalanceStoppedEventTest extends GridCommonAbstractTest {

    /** Cache name. */
    public static final String CACHE_NAME = "ON_HEAP_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<String, String> cacheCfg = new CacheConfiguration<String, String>();

        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cacheCfg.setBackups(1);

        cacheCfg.setName(CACHE_NAME);

        cacheCfg.setRebalanceDelay(0);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setIncludeEventTypes(concat(cfg.getIncludeEventTypes(), EventType.EVT_CACHE_REBALANCE_STOPPED));

        return cfg;
    }

    /**
     * Concatenates elements to an int array.
     *
     * @param arr Array.
     * @param obj One or more elements to concatenate.
     * @return Concatenated array.
     */
    protected static int[] concat(@Nullable int[] arr, int... obj) {
        int[] newArr;

        if (arr == null || arr.length == 0)
            newArr = obj;
        else {
            newArr = Arrays.copyOf(arr, arr.length + obj.length);

            System.arraycopy(obj, 0, newArr, arr.length, obj.length);
        }

        return newArr;
    }

    /** Partitions storage. */
    private static int[] partitionsStorage;

    /** Latch. */
    private static CountDownLatch latch = new CountDownLatch(1);

    public void test() throws Exception {
        try {
            Ignite instance = startGrid(0);

            instance.events().localListen(new rebelanceListener(), EventType.EVT_CACHE_REBALANCE_STOPPED);

            Affinity aff = instance.affinity(CACHE_NAME);

            partitionsStorage = aff.primaryPartitions(instance.cluster().localNode());

            System.out.println("Partitions : " + Arrays.toString(partitionsStorage));

            startGrid(2);

            assertTrue(latch.await(10, TimeUnit.SECONDS));

            int[] partitions = aff.primaryPartitions(instance.cluster().localNode());

            System.out.println("Partitions after rebalance : " + Arrays.toString(partitions));


            assertFalse(Arrays.toString(partitions).equals(Arrays.toString(partitionsStorage)));

        }
        finally {
            stopAllGrids();
        }

    }

    /**
     * Listener.
     */
    private static class rebelanceListener implements IgnitePredicate<EventAdapter> {
        /**
         * Ignite node.
         */
        @IgniteInstanceResource
        private Ignite node;

        /** {@inheritDoc} */
        @Override public boolean apply(EventAdapter evt) {
            Boolean isClient = null;

            switch (evt.type()) {
                case EventType.EVT_CACHE_REBALANCE_STOPPED:
                    if (evt instanceof CacheRebalancingEvent) {
                        CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent) evt;

                        isClient = cacheEvt.discoveryNode().attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE);

                        if (isClient != null && !isClient
                            && cacheEvt.cacheName().equals(CACHE_NAME)
                            && (cacheEvt.discoveryEventType() == EventType.EVT_NODE_JOINED
                            || cacheEvt.discoveryEventType() == EventType.EVT_NODE_LEFT
                            || cacheEvt.discoveryEventType() == EventType.EVT_NODE_FAILED)) {

                            //Rebalance has been finished?
                            System.out.println("Rebalance stopped.");

                            Affinity aff = node.affinity(CACHE_NAME);

                            int[] partitions = aff.primaryPartitions(node.cluster().localNode());

                            System.out.println("Current partitions : " + Arrays.toString(partitions));

                            assertFalse(Arrays.toString(partitions).equals(Arrays.toString(partitionsStorage)));

                            latch.countDown();

                        }
                    }

                    break;
            }

            return true;
        }
    }


}
