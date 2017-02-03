package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ClientReconnectContinuousQueryTest extends GridCommonAbstractTest {
    /** Client index. */
    private static final int CLIENT_IDX = 1;

    /** Puts before reconnect. */
    private static final int PUTS_BEFORE_RECONNECT = 50;

    /** Puts after reconnect. */
    private static final int PUTS_AFTER_RECONNECT = 50;

    /** Recon latch. */
    private static final CountDownLatch reconLatch = new CountDownLatch(1);

    /** Discon latch. */
    private static final CountDownLatch disconLatch = new CountDownLatch(1);

    /** Updater received. */
    private static final CountDownLatch updaterReceived = new CountDownLatch(PUTS_BEFORE_RECONNECT);

    /** Receiver after reconnect. */
    private static final CountDownLatch receiverAfterReconnect = new CountDownLatch(PUTS_AFTER_RECONNECT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi communicationSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

        communicationSpi.setSlowClientQueueLimit(50);
        communicationSpi.setIdleConnectionTimeout(300_000);

        if (getTestGridName(CLIENT_IDX).equals(gridName))
            cfg.setClientMode(true);
        else {
            CacheConfiguration ccfg = defaultCacheConfiguration();

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * Test client reconnect to alive grid.
     */
    public void testClientReconnect() throws Exception {
        try {
            startGrids(2);

            IgniteEx client = grid(CLIENT_IDX);

            client.events().localListen(new DisconListener(), EventType.EVT_CLIENT_NODE_DISCONNECTED);

            client.events().localListen(new ReconListener(), EventType.EVT_CLIENT_NODE_RECONNECTED);

            IgniteCache cache = client.cache(null);

            ContinuousQuery qry = new ContinuousQuery();

            qry.setLocalListener(new CQListener());

            cache.query(qry);

            putSomeKeys(PUTS_BEFORE_RECONNECT);

            info("updaterReceived Count: " + updaterReceived.getCount());

            assertTrue(updaterReceived.await(10_000, TimeUnit.MILLISECONDS));

            skipRead(client, true);

            putSomeKeys(1_000);

            assertTrue(disconLatch.await(10_000, TimeUnit.MILLISECONDS));

            skipRead(client, false);

            assertTrue(reconLatch.await(10_000, TimeUnit.MILLISECONDS));

            putSomeKeys(PUTS_AFTER_RECONNECT /** 10000*/);

            info("receiverAfterReconnect Count: " + receiverAfterReconnect.getCount());

            assertTrue(receiverAfterReconnect.await(10_000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }

    }

    private static class ReconListener implements IgnitePredicate<Event> {

        @Override public boolean apply(Event event) {
            reconLatch.countDown();

            return false;
        }
    }

    private static class DisconListener implements IgnitePredicate<Event> {

        @Override public boolean apply(Event event) {
            disconLatch.countDown();

            return false;
        }
    }

    private static class CQListener implements CacheEntryUpdatedListener {

        @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
            if (reconLatch.getCount() != 0)
                for (Object o : iterable)
                    updaterReceived.countDown();
            else
                for (Object o : iterable)
                    receiverAfterReconnect.countDown();
        }
    }

    private void putSomeKeys(int count) {
        IgniteEx ignite = grid(0);

        IgniteCache srvCache = ignite.cache(null);

        for (int i = 0; i < count; i++)
            srvCache.put(0, i);
    }

    /**
     * @param igniteClient Ignite client.
     * @param skip Skip.
     */
    private void skipRead(IgniteEx igniteClient, boolean skip) {
        GridIoManager ioMgr = igniteClient.context().io();

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)((Object[])U.field(ioMgr, "spis"))[0];

        GridNioServer nioSrvr = U.field(commSpi, "nioSrvr");

        GridTestUtils.setFieldValue(nioSrvr, "skipRead", skip);
    }

}
