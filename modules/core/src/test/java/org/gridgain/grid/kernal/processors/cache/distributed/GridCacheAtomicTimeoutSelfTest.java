/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests timeout exception when message gets lost.
 */
public class GridCacheAtomicTimeoutSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    public static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setDistributionMode(PARTITIONED_ONLY);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setNetworkTimeout(3000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            final GridKernal grid = (GridKernal)grid(i);

            TestCommunicationSpi commSpi = (TestCommunicationSpi)grid.configuration().getCommunicationSpi();

            commSpi.skipNearRequest = false;
            commSpi.skipNearResponse = false;
            commSpi.skipDhtRequest = false;
            commSpi.skipDhtResponse = false;

            GridTestUtils.retryAssert(log, 10, 100, new CA() {
                @Override public void apply() {
                    assertTrue(grid.internalCache().context().mvcc().atomicFutures().isEmpty());
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearUpdateRequestLost() throws Exception {
        Ignite ignite = grid(0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        GridCache<Object, Object> cache = ignite.cache(null);

        int key = keyForTest();

        cache.put(key, 0);

        commSpi.skipNearRequest = true;

        IgniteFuture<Object> fut = cache.putAsync(key, 1);

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(1).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (GridCacheAtomicUpdateTimeoutException ignore) {
            // Expected exception.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearUpdateResponseLost() throws Exception {
        Ignite ignite = grid(0);

        GridCache<Object, Object> cache = ignite.cache(null);

        int key = keyForTest();

        cache.put(key, 0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        commSpi.skipNearResponse = true;

        IgniteFuture<Object> fut = cache.putAsync(key, 1);

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(0).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (GridCacheAtomicUpdateTimeoutException ignore) {
            // Expected exception.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtUpdateRequestLost() throws Exception {
        Ignite ignite = grid(0);

        GridCache<Object, Object> cache = ignite.cache(null);

        int key = keyForTest();

        cache.put(key, 0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        commSpi.skipDhtRequest = true;

        IgniteFuture<Object> fut = cache.putAsync(key, 1);

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(2).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (GridException e) {
            assertTrue("Invalid exception thrown: " + e, X.hasCause(e, GridCacheAtomicUpdateTimeoutException.class)
                || X.hasSuppressed(e, GridCacheAtomicUpdateTimeoutException.class));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtUpdateResponseLost() throws Exception {
        Ignite ignite = grid(0);

        GridCache<Object, Object> cache = ignite.cache(null);

        int key = keyForTest();

        cache.put(key, 0);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(2).configuration().getCommunicationSpi();

        commSpi.skipDhtResponse = true;

        IgniteFuture<Object> fut = cache.putAsync(key, 1);

        Map<UUID, GridCommunicationClient> clients = U.field(commSpi, "clients");

        GridTcpNioCommunicationClient client = (GridTcpNioCommunicationClient)clients.get(grid(1).localNode().id());

        client.session().close().get();

        try {
            fut.get();

            fail();
        }
        catch (GridException e) {
            assertTrue("Invalid exception thrown: " + e, X.hasCause(e, GridCacheAtomicUpdateTimeoutException.class)
                || X.hasSuppressed(e, GridCacheAtomicUpdateTimeoutException.class));
        }
    }

    /**
     * @return Key for test;
     */
    private int keyForTest() {
        int i = 0;

        GridCacheAffinity<Object> aff = grid(0).cache(null).affinity();

        while (!aff.isPrimary(grid(1).localNode(), i) || !aff.isBackup(grid(2).localNode(), i))
            i++;

        return i;
    }

    /**
     * Communication SPI that will count single partition update messages.
     */
    private static class TestCommunicationSpi extends GridTcpCommunicationSpi {
        /** */
        private boolean skipNearRequest;

        /** */
        private boolean skipNearResponse;

        /** */
        private boolean skipDhtRequest;

        /** */
        private boolean skipDhtResponse;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
            throws IgniteSpiException {
            if (!skipMessage((GridIoMessage)msg))
                super.sendMessage(node, msg);
        }

        /**
         * Checks if message should be skipped.
         *
         * @param msg Message.
         */
        private boolean skipMessage(GridIoMessage msg) {
            return msg.message() instanceof GridNearAtomicUpdateRequest && skipNearRequest
                || msg.message() instanceof GridNearAtomicUpdateResponse && skipNearResponse
                || msg.message() instanceof GridDhtAtomicUpdateRequest && skipDhtRequest
                || msg.message() instanceof GridDhtAtomicUpdateResponse && skipDhtResponse;
        }
    }

}
