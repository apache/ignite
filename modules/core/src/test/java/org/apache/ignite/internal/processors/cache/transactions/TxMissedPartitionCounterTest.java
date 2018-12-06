package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.ByteArrayDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.max;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test data loss on recovery due to missed updates with lower partition counter.
 */
public class TxMissedPartitionCounterTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    public static final int COUNT = 5000;

    /** */
    private int backups;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId("node" + igniteInstanceName);
        cfg.setFailureHandler(new StopNodeFailureHandler());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalSegmentSize(12 * MB).setWalMode(LOG_ONLY).setPageSize(1024).setCheckpointFrequency(10000000000L).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setFailureDetectionTimeout(60000);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(backups);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * Tests partition consistency with lost update and historical rebalance.
     * @throws Exception if failed.
     */
    public void testMissedPartitionCounterWALRebalance() throws Exception {
        try {
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

            doTestMissedPartitionCounter(false);

            assertFalse(IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances().isEmpty());
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

            IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();
        }
    }

    /**
     * Tests partition consistency with lost update and historical rebalance.
     * @throws Exception if failed.
     */
    public void testMissedPartitionCounter() throws Exception {
        doTestMissedPartitionCounter(false);
    }

    /**
     * Tests partition consistency with reordering of updates on backup.
     * Primary order: 1(keys[0]) | cp | 2(keys[1])
     * Backup order: 2 fail node
     */
    private void doTestMissedPartitionCounter(boolean skipCheckpointOnNodeStop) throws Exception {
        try {
            IgniteEx ex0 = startGrid(0);
            startGrid(1);
            startGrid(2);

            ex0.cluster().active(true);

            awaitPartitionMapExchange();

            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            int part = 0;

            final int txCnt = 2;

            List<Integer> keys = loadDataToPartition(part, DEFAULT_CACHE_NAME, 5000, 0, txCnt);

            CountDownLatch[] latches = new CountDownLatch[txCnt - 1];

            Arrays.setAll(latches, val -> {
                return new CountDownLatch(1);
            });

            forceCheckpoint();

            Ignite primaryNode = primaryNode(keys.get(0), DEFAULT_CACHE_NAME);

            IgniteEx backupNode = (IgniteEx)backupNodes(keys.get(0), DEFAULT_CACHE_NAME).get(1);

            TestRecordingCommunicationSpi.spi(primaryNode).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (node.id().equals(backupNode.cluster().localNode().id()) && msg instanceof GridDhtTxFinishRequest) {
                        GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)msg;

                        int cntr = (int)req.partUpdateCounters().get(0) - COUNT;

//                        try {
//                            forceCheckpoint(primaryNode);
//                        }
//                        catch (IgniteCheckedException e) {
//                            fail();
//                        }

                        if (cntr != txCnt)
                            latches[cntr - 1].countDown();

                        return true;
                    }

                    return false;
                }
            });

            // Reorder updates on primary and backup.
            IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        TestRecordingCommunicationSpi.spi(primaryNode).waitForBlocked(txCnt);
                    }
                    catch (InterruptedException e) {
                        fail();
                    }
//
//                    IgniteCacheProxy<?, ?> cache = backupNode.context().cache().jcache(DEFAULT_CACHE_NAME);
//
//                    GridDhtCacheAdapter<?, ?> dht = dht(cache);
//
//                    GridDhtPartitionTopology top = dht.topology();
//
//                    @Nullable GridDhtLocalPartition part0 = top.localPartition(0);
//
//                    long uc0 = part0.updateCounter();
//                    long iuc0 = part0.initialUpdateCounter();
//                    long size0 = part0.fullSize();

                    TestRecordingCommunicationSpi.spi(primaryNode).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
                        @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                            GridIoMessage ioMsg = objects.get2();

                            GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)ioMsg.message();

                            long cntr = req.partUpdateCounters().get(0);

                            return cntr - COUNT == 2;
                        }
                    });

                    doSleep(3000); // Give time to commit. TODO: dispose of sleep.

//                    GridDhtLocalPartition part1 = top.localPartition(0);
//
//                    long uc1 = part1.updateCounter();
//                    long iuc1 = part1.initialUpdateCounter();
//                    long size1 = part1.fullSize();

                    // Skipping commit on node stop will cause logical recovery on node start afterwards.
                    // TODO add test for stopping node during checkpoint.
                    if (skipCheckpointOnNodeStop) {
                        GridCacheDatabaseSharedManager db =
                            (GridCacheDatabaseSharedManager)backupNode.context().cache().context().database();

                        db.enableCheckpoints(false);
                    }

                    stopGrid(backupNode.name());

                    TestRecordingCommunicationSpi.spi(primaryNode).stopBlock(); // Finish first tx
                }
            });

            AtomicInteger id = new AtomicInteger();

            IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    int idx = id.getAndIncrement();

                    if (idx > 0)
                        assertTrue(U.await(latches[idx - 1], 10, TimeUnit.SECONDS));

                    client.cache(DEFAULT_CACHE_NAME).put(keys.get(idx), idx);

                    tx.commit();
                }
                catch (IgniteInterruptedCheckedException e) {
                    fail();
                }
            }, txCnt, "tx-thread");

            fut.get();
            fut0.get();

            for (int i = 0; i < txCnt; i++)
                assertEquals(i, client.cache(DEFAULT_CACHE_NAME).get(keys.get(i)));

            stopAllGrids();

            Ignite ex = startGrid(0);
            ex.cluster().active(true);

            startGrid(1);

            IdleVerifyResultV2 res = idleVerify(grid(0), DEFAULT_CACHE_NAME);

            if (res.hasConflicts()) {
                StringBuilder b = new StringBuilder();

                res.print(b::append);

                fail(b.toString());

            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests partition consistency with lost update and full rebalance.
     */
//    public void testMissedPartitionCounter() throws Exception {
//        try {
//            startGridsMultiThreaded(GRID_CNT);
//
//            IgniteEx client = startGrid("client");
//
//            assertNotNull(client.cache(DEFAULT_CACHE_NAME));
//
//            int part = 0;
//
//            final int txCnt = 2;
//
//            List<Integer> keys = loadDataToPartition(part, DEFAULT_CACHE_NAME, COUNT, 0, txCnt);
//
//            forceCheckpoint();
//
//            Ignite backupNode = backupNode(keys.get(0), DEFAULT_CACHE_NAME);
//
//            Map<UUID, T3<CountDownLatch, Set<Long>, Boolean>> latches = new ConcurrentHashMap<>();
//
//            for (Ignite ignite : G.allGrids()) {
//                if (ignite != backupNode)
//                    continue;
//
//                IgniteEx igniteEx = (IgniteEx)ignite;
//
//                GridCacheSharedContext<Object, Object> ctx = igniteEx.context().cache().context();
//
//                IgniteTxManager tm = ctx.tm();
//
//                tm.walWriteListener(new WALWriteListener() {
//                    @Override public void beforeWrite(List<DataEntry> entries) {
//                        try {
//                            long cntr = entries.get(0).partitionCounter();
//
//                            T3<CountDownLatch, Set<Long>, Boolean> val = new T3<>(new CountDownLatch(txCnt), new ConcurrentSkipListSet<>(), Boolean.FALSE);
//                            T3<CountDownLatch, Set<Long>, Boolean> oldVal = latches.putIfAbsent(ignite.cluster().localNode().id(), val);
//
//                            if (oldVal != null)
//                                val = oldVal;
//
//                            assertNotNull(val.get1());
//                            assertNotNull(val.get2());
//
//                            val.get2().add(cntr);
//
//                            val.get1().countDown();
//                            assertTrue(U.await(val.get1(), 10, TimeUnit.SECONDS));
//
//                            // Compute max counter and fail nodes with lesser counter before writing to WAL.
//                            long maxCntr = max(val.get2());
//
//                            // Fail nodes with lowest counters.
//                            if (cntr < maxCntr) {
//                                // Wait until max counter is written.
//                                synchronized (val) {
//                                    while (val.get3() != Boolean.TRUE)
//                                        U.wait(val);
//                                }
//
//                                throw new RuntimeException("Fail node");
//                            }
//                        }
//                        catch (IgniteInterruptedCheckedException e) {
//                            fail();
//                        }
//                    }
//
//                    @Override public void afterWrite(List<DataEntry> entries) {
//                        T3<CountDownLatch, Set<Long>, Boolean> val = latches.get(ignite.cluster().localNode().id());
//
//                        assertNotNull(val.get2());
//
//                        long maxCntr = max(val.get2());
//
//                        // Unblock waiters after write of max counter.
//                        if (entries.get(0).partitionCounter() == maxCntr) {
//                            synchronized (val) {
//                                val.set3(Boolean.TRUE);
//
//                                val.notifyAll();
//                            }
//                        }
//                    }
//                });
//            }
//
//            AtomicInteger id = new AtomicInteger();
//
//            IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
//                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
//                    int idx = id.getAndIncrement();
//
//                    client.cache(DEFAULT_CACHE_NAME).put(keys.get(idx), idx);
//
//                    tx.commit();
//                }
//            }, txCnt, "tx-thread");
//
//            fut.get();
//
//            // Wait for backups stop.
//            waitForTopology(GRID_CNT);
//
//            awaitPartitionMapExchange();
//
//            for (int i = 0; i < txCnt; i++)
//                assertEquals(i, client.cache(DEFAULT_CACHE_NAME).get(keys.get(i)));
//
//            forceCheckpoint();
//
//            stopAllGrids();
//
//            Ignite ex = startGridsMultiThreaded(GRID_CNT);
//
//            awaitPartitionMapExchange();
//
//            IdleVerifyResultV2 res = idleVerify(grid(0), DEFAULT_CACHE_NAME);
//
//            if (res.hasConflicts()) {
//                StringBuilder b = new StringBuilder();
//
//                res.print(b::append);
//
//                fail(b.toString());
//            }
//        }
//        finally {
//            stopAllGrids();
//        }
//    }

    public void testBasicCounterAssignmentOnPrimary() throws Exception {
        try {
            backups = 0;

            Ignite crd = startGridsMultiThreaded(2);

            List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), 0, 10);

            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            try(Transaction tx = client.transactions().txStart()) {
                for (Integer key : keys.subList(0, 7))
                    client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                tx.commit();
            }

            printPartitionState(DEFAULT_CACHE_NAME, 0);

            try(Transaction tx = client.transactions().txStart()) {
                for (Integer key : keys.subList(7, 10))
                    client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                tx.commit();
            }

            printPartitionState(DEFAULT_CACHE_NAME, 0);

//            prim.close();
//
//            awaitPartitionMapExchange();
//
//            // TODO FIXME reserveCntr is wrong !!!
//            printPartitionState(DEFAULT_CACHE_NAME, 0);
        }
        finally {
            stopAllGrids();
        }
    }

    public void testFailPrimary() throws Exception {
        try {
            Ignite crd = startGridsMultiThreaded(3);

            int key = 0;

            Ignite prim = primaryNode(key, DEFAULT_CACHE_NAME);

            IgniteEx client = startGrid("client");


            TestRecordingCommunicationSpi.spi(prim).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridNearTxPrepareResponse;
                }
            });

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        TestRecordingCommunicationSpi.spi(prim).waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    prim.close();

                    try {
                        awaitPartitionMapExchange();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            try(Transaction tx = client.transactions().txStart()) {
                client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                tx.commit();
            }

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    public void testFailPrimary2() throws Exception {
        try {
            Ignite crd = startGrids(3);
            crd.cluster().active(true);
            awaitPartitionMapExchange();

            int key = 0;
            int key1 = 1;

            Ignite prim0 = primaryNode(key, DEFAULT_CACHE_NAME);
            Ignite backup0 = backupNode(key, DEFAULT_CACHE_NAME);

            Ignite prim1 = primaryNode(key1, DEFAULT_CACHE_NAME);
            Ignite backup1 = backupNode(key1, DEFAULT_CACHE_NAME);

            IgniteEx client = startGrid("client");

            TestRecordingCommunicationSpi.spi(prim0).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridNearTxPrepareResponse;
                }
            });

            TestRecordingCommunicationSpi.spi(prim1).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridNearTxPrepareResponse;
                }
            });

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        TestRecordingCommunicationSpi.spi(prim0).waitForBlocked();
                        TestRecordingCommunicationSpi.spi(prim1).waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    TestRecordingCommunicationSpi.spi(prim0).stopBlock();

                    try {
                        U.sleep(2000);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
                    }

                    prim1.close();

                    try {
                        awaitPartitionMapExchange();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            try(Transaction tx = client.transactions().txStart()) {
                client.cache(DEFAULT_CACHE_NAME).put(key, 0);
                client.cache(DEFAULT_CACHE_NAME).put(key1, 0);

                tx.commit();
            }

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    public void testFailPrimary3() throws Exception {
        try {
            Ignite crd = startGrids(3);
            crd.cluster().active(true);
            awaitPartitionMapExchange();

            int key = 0;

            Ignite prim0 = primaryNode(key, DEFAULT_CACHE_NAME);

            IgniteEx client = startGrid("client");

            TestRecordingCommunicationSpi.spi(client).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridNearTxFinishRequest;
                }
            });

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        TestRecordingCommunicationSpi.spi(client).waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    prim0.close();

                    try {
                        awaitPartitionMapExchange();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            try(Transaction tx = client.transactions().txStart()) {
                client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                tx.commit();
            }

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    public void testSaveGaps() throws Exception {
        try {
            IgniteEx grid = (IgniteEx)startGridsMultiThreaded(1);
            int key = 0;
            grid.cache(DEFAULT_CACHE_NAME).put(key, 0);

            GridCacheContext<Object, Object> cctx = internalCache(0).context();
            @Nullable GridDhtLocalPartition part = cctx.topology().localPartition(0);

            assertTrue(grid.affinity(DEFAULT_CACHE_NAME).partition(key) == 0);

            GridCacheOffheapManager.GridCacheDataStore rowStore = (GridCacheOffheapManager.GridCacheDataStore)part.dataStore();

            FreeList<ByteArrayDataRow> list = U.field(rowStore, "freeList");

            grid.context().cache().context().database().checkpointReadLock();

            try {
                Random r = new Random();
                byte[] buf0 = new byte[5555];
                r.nextBytes(buf0);

                // insert
                ByteArrayDataRow row = new ByteArrayDataRow(cctx.cacheObjectContext(), part.id(), 0, buf0);
                list.insertDataRow(row);

                assertTrue(Arrays.equals(buf0, getData(part.group(), row.link(), part.id())));

                long link0 = row.link();

//                // update in-place
//                byte[] buf1 = new byte[555];
//                r.nextBytes(buf1);
//
//                row = new ByteArrayDataRow(part.id(), 0, buf1);
//                list.updateDataRow(link0, row);
//
//                assertTrue(Arrays.equals(buf1, getData(part.group(), link0, part.id())));

                // remove
                list.removeDataRowByLink(link0);

                // update and shrink
                byte[] buf2 = new byte[5555];
                r.nextBytes(buf2);

                row = new ByteArrayDataRow(cctx.cacheObjectContext(), part.id(), 0, buf2);
                list.insertDataRow(row);

                assertTrue(Arrays.equals(buf2, getData(part.group(), row.link(), part.id())));

                System.out.println();

                //assertTrue(Arrays.equals(buf1, getData(part.group(), row.link(), part.id())));

//                PartitionUpdateCounter pc = new PartitionUpdateCounter(log);
//
//                pc.reserve(2);
//                pc.reserve(6);
//                pc.reserve(3);
//                pc.reserve(1);
//
//                byte[] rawData = pc.getBytes();
//
//                ByteArrayDataRow row0 = new ByteArrayDataRow(part.id(), 0, rawData);
//
//                list.insertDataRow(row0);
//
//                System.out.println(row0.link());
//
//                PartitionUpdateCounter pc2 = new PartitionUpdateCounter(log);
//                pc2.init(0, getData(part.group(), row0.link(), part.id()));
//
//                assertEquals(pc.get(), pc2.get());
//
//                assertEquals(pc.holes(), pc2.holes());
            }
            finally {
                grid.context().cache().context().database().checkpointReadUnlock();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    public byte[] getData(CacheGroupContext grp, long link, int partId) {
        try {
            ByteArrayDataRow row1 = new ByteArrayDataRow(grp, link, partId);
            return row1.value().valueBytes(null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override protected long getTestTimeout() {
        return 10000000000000000L;
    }
}
