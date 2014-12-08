/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 *
 * Forum example <a
 * href="http://www.gridgainsystems.com/jiveforums/thread.jspa?threadID=1449">
 * http://www.gridgainsystems.com/jiveforums/thread.jspa?threadID=1449</a>
 */
public class GridCacheDhtPreloadMessageCountTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 1000;

    /** Preload mode. */
    private GridCachePreloadMode preloadMode = GridCachePreloadMode.SYNC;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        assert preloadMode != null;

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setPreloadMode(preloadMode);
        cc.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 521));
        cc.setBackups(1);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);
        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);

        c.setCommunicationSpi(new TestCommunicationSpi());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAutomaticPreload() throws Exception {
        Ignite g0 = startGrid(0);

        int cnt = KEY_CNT;

        GridCache<String, Integer> c0 = g0.cache(null);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        U.sleep(1000);

        GridCache<String, Integer> c1 = g1.cache(null);
        GridCache<String, Integer> c2 = g2.cache(null);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)g0.configuration().getCommunicationSpi();
        TestCommunicationSpi spi1 = (TestCommunicationSpi)g1.configuration().getCommunicationSpi();
        TestCommunicationSpi spi2 = (TestCommunicationSpi)g2.configuration().getCommunicationSpi();

        info(spi0.sentMessages().size() + " " + spi1.sentMessages().size() + " " + spi2.sentMessages().size());

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /**
     * Checks if keys are present.
     *
     * @param c Cache.
     * @param keyCnt Key count.
     */
    private void checkCache(GridCache<String, Integer> c, int keyCnt) {
        Ignite g = c.gridProjection().ignite();

        for (int i = 0; i < keyCnt; i++) {
            String key = Integer.toString(i);

            if (c.affinity().isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(Integer.valueOf(i), c.peek(key));
        }
    }

    /**
     * Communication SPI that will count single partition update messages.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Recorded messages. */
        private Collection<GridDhtPartitionsSingleMessage> sentMsgs = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
            throws IgniteSpiException {
            recordMessage((GridIoMessage)msg);

            super.sendMessage(node, msg);
        }

        /**
         * @return Collection of sent messages.
         */
        public Collection<GridDhtPartitionsSingleMessage> sentMessages() {
            return sentMsgs;
        }

        /**
         * Adds message to a list if message is of correct type.
         *
         * @param msg Message.
         */
        private void recordMessage(GridIoMessage msg) {
            if (msg.message() instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage partSingleMsg = (GridDhtPartitionsSingleMessage)msg.message();

                sentMsgs.add(partSingleMsg);
            }
        }
    }
}
