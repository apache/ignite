/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Tests the local affinity recalculation exchange in case of concurrent completing rebalance.
 */
public class IgniteBaselineNodeLeaveOnRebalance extends GridCommonAbstractTest {
    private static final AtomicBoolean delayEvt = new AtomicBoolean();

    private static final AtomicBoolean checkLastSupplyMsg = new AtomicBoolean();

    private static final CountDownLatch latch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(300L * 1024 * 1024)
                    .setMaxSize(300L * 1024 * 1024))
                .setWalMode(WALMode.LOG_ONLY)
        );

        CacheConfiguration<Integer, Long> ccfg = new CacheConfiguration<Integer, Long>()
            .setName(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);

        cfg.setCacheConfiguration(ccfg);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setGridLogger(new NullLogger());

        cfg.setCommunicationSpi(new TestTcpCommunication());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        delayEvt.set(false);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNodeLeaveOnCompletingRebalance2() throws Exception {
        Ignite ignite = startGridsMultiThreaded(4);

        ignite.cluster().active(true);

        stopGrid(3, true);

        IgniteCache<Integer, Long> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10000; i++)
            cache.put(i, new Random().nextLong());

        IgniteEx grid = startGrid(3);

        System.out.println("\nMY START NODE 3 AND READY MOVING PARTS\n");
        printPartitionState(cache);

        stopGrid(2);

        printPartitionState(cache);

        U.sleep(10000);

        printPartitionState(cache);
    }


    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNodeLeaveOnCompletingRebalance() throws Exception {
        Ignite ignite = startGridsMultiThreaded(4);

        ignite.cluster().active(true);

        ArrayList<Integer> parts = new ArrayList<>();

        List<List<ClusterNode>> assignment = grid(3).context().cache().jcache(DEFAULT_CACHE_NAME)
            .context().affinity().idealAssignment();

        for (int p = 0; p < assignment.size(); p++)
            if (assignment.get(p).contains(grid(3).localNode()) && assignment.get(p).contains(grid(2).localNode()))
                parts.add(p);

        System.out.println("MY partsparts=" + parts);

        printPartitionState(grid(2).cache(DEFAULT_CACHE_NAME));

        String dn2DirName = grid(1).name().replace(".", "_");

        stopGrid(3, true);

        //Clean up the pds and WAL for second data node.
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/" + dn2DirName, true));

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/wal/" + dn2DirName, true));

        // Payload.
        IgniteCache<Integer, Long> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10000; i++)
            cache.put(i, new Random().nextLong());

        checkLastSupplyMsg.set(true);

        U.sleep(4000);

        IgniteEx grid = startGrid(3);

        System.out.println("MY CHECK1="+grid.context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().localPartition(parts.get(0)));
        System.out.println("MY CHECK11="+grid(0).context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().lostPartitions());

        System.out.println("\nMY START NODE 3 AND READY MOVING PARTS\n");
        System.out.println("MY ver2=" + grid(0).context().cache().context().exchange().readyAffinityVersion());
        printPartitionState(cache);

        latch.await();

        U.sleep(3000);
        stopGrid(2);
        System.out.println("MY CHECK2="+grid.context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().localPartition(parts.get(0)));
        System.out.println("MY CHECK22="+grid(0).context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().lostPartitions());

//        awaitPartitionMapExchange();
        System.out.println("MY CHECK3="+grid.context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().localPartition(parts.get(0)));
        System.out.println("MY CHECK33="+grid(0).context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().lostPartitions());

        System.out.println("MY AFTER STOP ver22=" + grid(0).context().cache().context().exchange().readyAffinityVersion());
        printPartitionState(cache);

        U.sleep(5000);
        System.out.println("MY ver3=" + grid(0).context().cache().context().exchange().readyAffinityVersion());
        printPartitionState(cache);
        System.out.println("MY CHECK4="+grid.context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().localPartition(parts.get(0)));
        System.out.println("MY CHECK44="+grid(0).context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().lostPartitions());
        System.out.println("MY CHECK444="+grid.context().cache().jcache(DEFAULT_CACHE_NAME).context().topology().lostPartitions());

    }

    /**
     * TcpCommunicationSpi with additional features needed for tests.
     */
    private class TestTcpCommunication extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            assert msg != null;

            if (checkLastSupplyMsg.get() && igniteInstanceName.equals(getTestIgniteInstanceName(2)) &&
                GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridDhtPartitionSupplyMessage) {

                    if (((GridDhtPartitionSupplyMessage)msg0).groupId() == ((IgniteEx)ignite).context().cache().jcache(DEFAULT_CACHE_NAME).context().groupId()){
                    latch.countDown();
                    System.out.println("\n\nMY LATCH DOWN\n\n");}
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
