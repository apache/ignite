package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.datastructures.GridCacheLockImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class TxHungReproducerTest extends IgniteCollectionAbstractTest {

    private static final String TRANSACTIONAL_CACHE_NAME = "tx_cache";

    @Override protected int gridCount() {
        return 3;
    }

    @Override protected CacheMode collectionCacheMode() {
        return CacheMode.REPLICATED;
    }

    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    Map<String, Thread> map = new ConcurrentHashMap<>();
    CountDownLatch lockWait = new CountDownLatch(1);
    CountDownLatch txWait = new CountDownLatch(1);
    UUID nearNodeId;
    UUID txNodeId;
    GridCacheLockImpl nearLockImpl;

    public void test() throws Exception {
        IgniteEx ig = grid(0);

        try (IgniteLock lock = ig.reentrantLock("MY_TEST_LOCK", false, true, true)) {
            GridCacheLockImpl impl = (GridCacheLockImpl)lock;

            IgniteEx primaryNode = primaryNode(impl);

            log.info("TEST LOG: PRIMARY node Id " + primaryNode.localNode().id());

            IgniteEx nearIgn = backupNode(impl);

            nearNodeId = nearIgn.localNode().id();

            log.info("TEST LOG: near node Id " + nearNodeId);

            IgniteLock nearLock = nearIgn.reentrantLock("MY_TEST_LOCK", false, true, false);

            nearLockImpl = (GridCacheLockImpl) nearLock;
            nearLock.lock();
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            }
            finally {
                nearLock.unlock();
            }

            GridTestUtils.runAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        map.put("cache-tx-thread", Thread.currentThread());
                        IgniteEx ig3 = startGrid(3);

                        txNodeId = ig3.localNode().id();

                        GridCacheAdapter cache = ig3.context().cache().internalCache(impl.viewCacheName());
                        try (GridNearTxLocal tx = CU.txStartInternal(cache.context(), cache, PESSIMISTIC, REPEATABLE_READ)) {
                            log.info("TEST LOG: get key");

                            cache.get(impl.key());

                            txWait.countDown();

                            lockWait.await();
                            log.info("TEST LOG: tx commit");
                            tx.commit();
                        }
                        return null;
                    }
                }, "cache-tx-thread"
            );

            IgniteInternalFuture fut = GridTestUtils.runAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        map.put("lock-thread", Thread.currentThread());

                        txWait.await();

                        try {
                            log.info("TEST LOG: before lock");

                            lockWait.countDown();

                            nearLock.lock();

                            log.info("TEST LOG: after lock");

                            return null;
                        }
                        finally {
                            nearLock.unlock();
                        }

                    }
                }, "lock-aquaere-thread"

            );

            while (!fut.isDone()) {
                TimeUnit.MILLISECONDS.sleep(500);
            }

            for(Ignite ignite: G.allGrids()) {

                GridCacheSharedContext cctx = ((IgniteEx)ignite).context().cache().context();
                log.info("TEST_LOG Pending transactions [node=" + ((IgniteEx)ignite).localNode().id() + "]:");

                for (IgniteInternalTx tx : cctx.tm().activeTransactions())
                    log.info("TEST_LOG >>> " + tx);
            }
        }
    }

    private IgniteEx backupNode(GridCacheLockImpl impl) {
        GridCacheAdapter cache = grid(0).context().cache().internalCache(impl.viewCacheName());

        for (Ignite ign : G.allGrids()) {
            IgniteEx ignEx = (IgniteEx)ign;
            if (cache.affinity().isBackup(ignEx.localNode(), impl.key()) &&
                !ignEx.localNode().id().equals(txNodeId))
                return ignEx;
        }

        return null;
    }

    private IgniteEx primaryNode(GridCacheLockImpl impl) {
        GridCacheAdapter cache = grid(0).context().cache().internalCache(impl.viewCacheName());

        for (Ignite ign : G.allGrids()) {
            IgniteEx ignEx = (IgniteEx)ign;
            if (cache.affinity().isPrimary(ignEx.localNode(), impl.key()))
                return ignEx;
        }

        return null;
    }

    private class TestCommunicationSpi extends TcpCommunicationSpi {
        public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (msg instanceof GridIoMessage) {
                final Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridDistributedBaseMessage ||
                    msg0 instanceof GridDistributedTxFinishResponse ||
                    msg0 instanceof GridDistributedTxFinishRequest)
                    log.info("TEST LOG: [node=" + node.id() + ", msg=" + msg0 + "]");

                if (msg0 instanceof GridNearLockRequest) {
                    GridNearLockRequest lockRq = (GridNearLockRequest)msg0;
                    if (txWait.getCount() == 0 && lockRq.subjectId().equals(nearNodeId))
                        lockWait.countDown();
                }

                if (msg0 instanceof GridNearLockResponse) {
                    if (lockWait.getCount() == 0 && node.id().equals(nearNodeId)) {
                        Thread th = map.get("lock-thread");
                        log.info("TEST LOG: interrupt thread [thread=" + th + "]");
                        nearLockImpl.setInterruptAll(true);
                        th.interrupt();
                    }

                }

            }

            super.sendMessage(node, msg, ackC);

        }

    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestCommunicationSpi com = new TestCommunicationSpi();
        com.setSharedMemoryPort(-1);
        cfg.setCommunicationSpi(com);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(collectionCacheMode());
        atomicCfg.setBackups(collectionConfiguration().getBackups());

        cfg.setAtomicConfiguration(atomicCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(TRANSACTIONAL_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }
}
