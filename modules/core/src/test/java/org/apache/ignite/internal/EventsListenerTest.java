package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.events.EventType.*;

/**
 */
public class EventsListenerTest extends GridCommonAbstractTest {
    /** Is client. */
    private boolean isClient = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setClientMode(isClient)
            .setIncludeEventTypes(EventType.EVT_NODE_JOINED,
                EVT_NODE_LEFT,
                EventType.EVT_NODE_FAILED,
                EventType.EVT_NODE_SEGMENTED);
    }

    /**
     *
     */
    public void testListenerClient() throws Exception {

        CountDownLatch remoteEvents = new CountDownLatch(1);
        CountDownLatch localEvents = new CountDownLatch(1);

        try {
            Ignite server0 = startGrid(0);

            isClient = true;

            Ignite client = startGrid(1);

            checkTopology(2);

            client.events().remoteListen(new LocalListener(localEvents),
                new RemoteDiscoveryEventFilter(remoteEvents),
                EventType.EVT_NODE_LEFT);

            isClient = false;

            Ignite server2 = startGrid(2);

            checkTopology(3);

            Thread.sleep(10_000);

            //restart
            stopGrid(0);
            server0 = startGrid(0);

            checkTopology(3);

            assertTrue(remoteEvents.await(5, TimeUnit.SECONDS));
            assertTrue(localEvents.await(5, TimeUnit.SECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class LocalListener implements IgniteBiPredicate<UUID, DiscoveryEvent> {
        /** Latch. */
        private CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        public LocalListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        public boolean apply(UUID uuid, DiscoveryEvent event) {
            latch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class RemoteDiscoveryEventFilter implements IgnitePredicate<DiscoveryEvent> {
        /** Latch. */
        private CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        public RemoteDiscoveryEventFilter(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(DiscoveryEvent event) {
            latch.countDown();

            return true;
        }
    }
}