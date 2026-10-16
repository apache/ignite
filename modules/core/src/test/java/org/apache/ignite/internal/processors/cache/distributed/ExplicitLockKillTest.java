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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.kill.KillLockCommandArg;
import org.apache.ignite.internal.management.kill.KillLockTask;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.job.GridJobWorker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.spi.systemview.view.CacheExplicitLockView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccManager.CACHE_EXPLICIT_LOCKS_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_LISTENER_PORT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests forcible release of explicit locks: kill of a lock and release of locks left by jobs and services.
 */
@RunWith(Parameterized.class)
public class ExplicitLockKillTest extends GridCommonAbstractTest {
    /** */
    private static final long TIMEOUT = 10_000L;

    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final String UNLOCK_ERR_MSG = "the lock has been released externally";

    /** */
    private static final String LOCK_ERR_MSG = "the lock request has been cancelled";

    /** Near cache flag. */
    @Parameterized.Parameter
    public boolean near;

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** Lock owner node. */
    private IgniteEx client;

    /** Executors of threads the locks are bound to. */
    private final List<ExecutorService> executors = new ArrayList<>();

    /** */
    @Parameterized.Parameters(name = "near={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setGridLogger(testLog);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(1);

        if (near)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        return cfg.setCacheConfiguration(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearStaticLog(GridJobWorker.class);

        startGrids(SRV_CNT);

        client = startClientGrid(SRV_CNT);

        if (near)
            client.getOrCreateNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            assertEquals(near, ((IgniteCacheProxy<?, ?>)node.cache(DEFAULT_CACHE_NAME)).context().isNear());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (ExecutorService exec : executors)
            exec.shutdownNow();

        executors.clear();

        testLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testKillAcquiredLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        ExecutorService exec = createExecutor();

        long threadId = threadId(exec);

        Lock lock = cache().lock(key);

        exec.submit(lock::lock).get();

        IgniteUuid xid = awaitXid(threadId);

        assertTrue(killLock(xid));

        // The lock is already killed.
        assertFalse(killLock(xid));

        assertLockFree(key);

        assertThrowsAnyCause(log, () -> exec.submit(lock::unlock).get(), IllegalStateException.class, UNLOCK_ERR_MSG);

        // The lock instance is still usable after the failed unlock.
        exec.submit(() -> {
            lock.lock();
            lock.unlock();
        }).get();

        assertLockFree(key);
    }

    /** */
    @Test
    public void testKillUnknownLock() throws Exception {
        assertFalse(killLock(IgniteUuid.randomUuid()));
    }

    /** */
    @Test
    public void testKillReentrantLock() throws Exception {
        int key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        int key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        ExecutorService exec = createExecutor();

        long threadId = threadId(exec);

        Lock lock = cache().lock(key1);

        // Key 1 is reentered by both locks while key 2 is locked with another lock version.
        Lock lockAll = cache().lockAll(Arrays.asList(key1, key2));

        exec.submit(() -> {
            lock.lock();
            lock.lock();
        }).get();

        IgniteUuid xid = awaitXid(threadId);

        exec.submit(lockAll::lock).get();

        assertTrue(killLock(xid));

        assertLockFree(key1);
        assertLockHeld(key2);

        // Key 2 is released even though the unlock fails due to the killed key 1.
        assertThrowsAnyCause(log, () -> exec.submit(lockAll::unlock).get(), IllegalStateException.class,
            UNLOCK_ERR_MSG);

        assertLockFree(key2);

        for (int i = 0; i < 2; i++) {
            assertThrowsAnyCause(log, () -> exec.submit(lock::unlock).get(), IllegalStateException.class,
                UNLOCK_ERR_MSG);
        }

        assertTrue(threadLocks(threadId).isEmpty());
    }

    /** */
    @Test
    public void testKillPendingLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        ExecutorService holderExec = createExecutor();
        ExecutorService waiterExec1 = createExecutor();
        ExecutorService waiterExec2 = createExecutor();

        long waiterThreadId1 = threadId(waiterExec1);
        long waiterThreadId2 = threadId(waiterExec2);

        Lock holderLock = cache().lock(key);

        holderExec.submit(holderLock::lock).get();

        Lock waiterLock1 = cache().lock(key);
        Lock waiterLock2 = cache().lock(key);

        Future<?> waitFut1 = waiterExec1.submit(waiterLock1::lock);
        Future<?> waitFut2 = waiterExec2.submit(() -> {
            waiterLock2.lockInterruptibly();

            return null;
        });

        IgniteUuid waiterXid1 = awaitXid(waiterThreadId1);
        IgniteUuid waiterXid2 = awaitXid(waiterThreadId2);

        assertFalse(waitFut1.isDone());
        assertFalse(waitFut2.isDone());

        assertTrue(killLock(waiterXid1));

        assertThrowsAnyCause(log, () -> waitFut1.get(TIMEOUT, MILLISECONDS), CacheException.class, LOCK_ERR_MSG);

        assertTrue(killLock(waiterXid2));

        assertThrowsAnyCause(log, () -> waitFut2.get(TIMEOUT, MILLISECONDS), CacheException.class, LOCK_ERR_MSG);

        assertTrue(threadLocks(waiterThreadId1).isEmpty());
        assertTrue(threadLocks(waiterThreadId2).isEmpty());

        // The lock owner is not affected.
        assertLockHeld(key);

        holderExec.submit(holderLock::unlock).get();

        assertLockFree(key);
    }

    /** */
    @Test
    public void testKillPartiallyAcquiredLock() throws Exception {
        int key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        int key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        ExecutorService holderExec = createExecutor();
        ExecutorService waiterExec = createExecutor();

        long waiterThreadId = threadId(waiterExec);

        Lock holderLock = cache().lock(key1);

        holderExec.submit(holderLock::lock).get();

        // Keys are locked one by one in the given order, so key 2 is acquired while key 1 is awaited.
        Lock waiterLock = cache().lockAll(Arrays.asList(key2, key1));

        Future<?> waitFut = waiterExec.submit(waiterLock::lock);

        IgniteUuid waiterXid = awaitXid(waiterThreadId);

        assertTrue(waitForCondition(() -> lockedInCluster(key2), TIMEOUT));

        assertFalse(waitFut.isDone());

        assertTrue(killLock(waiterXid));

        assertThrowsAnyCause(log, () -> waitFut.get(TIMEOUT, MILLISECONDS), CacheException.class,
            LOCK_ERR_MSG);

        assertLockFree(key2);
        assertLockHeld(key1);

        holderExec.submit(holderLock::unlock).get();

        assertLockFree(key1);
    }

    /**
     * Internal jobs mapped to the local node are executed synchronously in the thread that started the task, so locks
     * held by the thread before the job execution must not be released.
     */
    @Test
    public void testLocalInternalJobKeepsLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        LogListener lsnr = LogListener.matches("Job finished without releasing explicit locks").build();

        testLog.registerListener(lsnr);

        // Size task is internal and is mapped to data nodes, so the size job is executed in the user thread.
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        ExecutorService exec = createExecutor();

        Lock lock = cache.lock(key);

        exec.submit(() -> {
            lock.lock();

            try {
                cache.size();

                assertTrue(cache.isLocalLocked(key, true));
            }
            finally {
                lock.unlock();
            }
        }).get();

        assertLockFree(key);

        // The same for the size job executed in the thread of the job holding the lock.
        client.compute(client.cluster().forNodeId(grid(0).localNode().id())).run(new LockSizeJob(key));

        assertLockFree(key);

        assertFalse(lsnr.check());
    }

    /** */
    @Test
    public void testJobLeftLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        LogListener lsnr = LogListener.matches("Job finished without releasing explicit locks").build();

        testLog.registerListener(lsnr);

        client.compute(client.cluster().forNodeId(grid(0).localNode().id())).run(new LockJob(key));

        assertTrue(lsnr.check());

        assertLockFree(key);

        // The lock acquired and released by the job is not reported.
        LogListener lsnr2 = LogListener.matches("Job finished without releasing explicit locks").build();

        testLog.registerListener(lsnr2);

        client.compute(client.cluster().forNodeId(grid(0).localNode().id())).run(new LockUnlockJob(key));

        assertFalse(lsnr2.check());
    }

    /**
     * Locks left by a cancelled job are released while the job thread is interrupted. Lock is removed locally first and
     * then the unlock request is sent to the primary node, so the request must not fail due to the interruption.
     */
    @Test
    public void testInterruptedJobLeftLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        LogListener lsnr = LogListener.matches("Job finished without releasing explicit locks").build();

        testLog.registerListener(lsnr);

        // The job is executed locally, so its result is not affected by the interruption of the job thread.
        grid(0).compute(grid(0).cluster().forLocal()).run(new InterruptedLockJob(key, grid(1).localNode().id()));

        assertTrue(lsnr.check());

        assertLockFree(key);
    }

    /** */
    @Test
    public void testServiceLeftLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        LogListener lsnr = LogListener.matches("Service execution finished without releasing explicit locks").build();

        testLog.registerListener(lsnr);

        grid(0).services(grid(0).cluster().forLocal()).deployNodeSingleton("lock-service", new LockService(key));

        assertTrue(lsnr.check(TIMEOUT));

        assertLockFree(key);
    }

    /** */
    @Test
    public void testServiceMethodLeftLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        // A remote service method is invoked by the service proxy as a compute job (ServiceProxyCallable), so the left
        // lock is released and reported by the job worker.
        LogListener lsnr = LogListener.matches("Job finished without releasing explicit locks").build();

        testLog.registerListener(lsnr);

        grid(0).services(grid(0).cluster().forLocal()).deployNodeSingleton("lock-method-service",
            new LockMethodServiceImpl());

        client.services().serviceProxy("lock-method-service", LockMethodService.class, false).lock(key);

        assertTrue(lsnr.check());

        assertLockFree(key);
    }

    /** */
    @Test
    public void testThinClientServiceMethodLeftLock() throws Exception {
        int key = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        LogListener lsnr = LogListener.matches("Service method invocation finished without releasing explicit locks")
            .build();

        testLog.registerListener(lsnr);

        grid(0).services(grid(0).cluster().forLocal()).deployNodeSingleton("lock-method-service",
            new LockMethodServiceImpl());

        // The thin client is connected only to the node the service is deployed on, so the method is invoked locally
        // in the client connector thread.
        try (IgniteClient thinClient = Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:" + grid(0).localNode().attribute(CLIENT_LISTENER_PORT))
            .setPartitionAwarenessEnabled(false)
            .setClusterDiscoveryEnabled(false))
        ) {
            thinClient.services().serviceProxy("lock-method-service", LockMethodService.class).lock(key);
        }

        assertTrue(lsnr.check());

        assertLockFree(key);
    }

    /** @return Cache of the lock owner node. */
    private IgniteCache<Integer, Integer> cache() {
        return client.cache(DEFAULT_CACHE_NAME);
    }

    /** @return Single thread executor the locks are bound to. */
    private ExecutorService createExecutor() {
        ExecutorService exec = Executors.newSingleThreadExecutor();

        executors.add(exec);

        return exec;
    }

    /** @return ID of the executor thread. */
    private static long threadId(ExecutorService exec) throws Exception {
        return exec.submit(() -> Thread.currentThread().getId()).get();
    }

    /**
     * @param threadId Thread ID.
     * @return Explicit locks of the thread.
     */
    private List<CacheExplicitLockView> threadLocks(long threadId) {
        SystemView<CacheExplicitLockView> view = client.context().systemView().view(CACHE_EXPLICIT_LOCKS_VIEW);

        return U.arrayList(view.iterator(), view.size(), v -> v.threadId() == threadId);
    }

    /**
     * @param threadId Thread ID.
     * @return Xid of the lock acquired or awaited by the thread.
     */
    private IgniteUuid awaitXid(long threadId) throws Exception {
        assertTrue(waitForCondition(() -> !threadLocks(threadId).isEmpty(), TIMEOUT));

        return threadLocks(threadId).get(0).xid();
    }

    /**
     * @param xid Lock xid.
     * @return {@code True} if the lock was found and killed on the lock owner (client) node.
     */
    private boolean killLock(IgniteUuid xid) throws Exception {
        KillLockCommandArg arg = new KillLockCommandArg();

        arg.xid(xid);

        VisorTaskArgument<KillLockCommandArg> taskArg =
            new VisorTaskArgument<>(F.viewReadOnly(grid(0).cluster().nodes(), ClusterNode::id), arg, false);

        // Lock owner is a client node, so the task is executed on all nodes, not only on servers.
        UUID nodeId = grid(0).compute(grid(0).cluster()).execute(KillLockTask.class, taskArg).result();

        if (nodeId == null)
            return false;

        assertEquals(client.localNode().id(), nodeId);

        return true;
    }

    /** @param key Key that must be free in the cluster. */
    private void assertLockFree(int key) throws Exception {
        Lock lock = grid(SRV_CNT - 1).cache(DEFAULT_CACHE_NAME).lock(key);

        // Lock release is not synchronous with the kill or job completion, so wait for it.
        assertTrue(lock.tryLock(TIMEOUT, MILLISECONDS));

        lock.unlock();
    }

    /** @param key Key that must be locked in the cluster. */
    private void assertLockHeld(int key) {
        assertTrue(lockedInCluster(key));
    }

    /**
     * @param key Key.
     * @return {@code True} if the key is locked in the cluster.
     */
    private boolean lockedInCluster(int key) {
        Lock lock = grid(0).cache(DEFAULT_CACHE_NAME).lock(key);

        if (!lock.tryLock())
            return true;

        lock.unlock();

        return false;
    }

    /** Job that locks a key and does not unlock it. */
    private static class LockJob implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /** @param key Key. */
        LockJob(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.cache(DEFAULT_CACHE_NAME).lock(key).lock();
        }
    }

    /** Job that gets cache size while holding a lock and checks that the lock is still held after that. */
    private static class LockSizeJob implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /** @param key Key. */
        LockSizeJob(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            Lock lock = cache.lock(key);

            lock.lock();

            try {
                cache.size();

                assertTrue(cache.isLocalLocked(key, true));
            }
            finally {
                // Fails if the lock is released by the size job.
                lock.unlock();
            }
        }
    }

    /**
     * Job that locks a key and finishes with the interrupted thread as a cancelled job does. Connections to the primary
     * node are closed, so the unlock request requires a new connection to be established by the interrupted thread.
     */
    private static class InterruptedLockJob implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /** */
        private final UUID primaryId;

        /**
         * @param key Key.
         * @param primaryId Primary node ID for the key.
         */
        InterruptedLockJob(int key, UUID primaryId) {
            this.key = key;
            this.primaryId = primaryId;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.cache(DEFAULT_CACHE_NAME).lock(key).lock();

            ConnectionClientPool connPool = U.field(ignite.configuration().getCommunicationSpi(), "clientPool");

            try {
                connPool.forceCloseConnection(primaryId);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            Thread.currentThread().interrupt();
        }
    }

    /** Job that locks and unlocks a key. */
    private static class LockUnlockJob implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /** @param key Key. */
        LockUnlockJob(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Lock lock = ignite.cache(DEFAULT_CACHE_NAME).lock(key);

            lock.lock();
            lock.unlock();
        }
    }

    /** */
    public interface LockMethodService {
        /** @param key Key to lock and not unlock. */
        public void lock(int key);
    }

    /** Service with a method that locks a key and does not unlock it. */
    public static class LockMethodServiceImpl implements LockMethodService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void lock(int key) {
            ignite.cache(DEFAULT_CACHE_NAME).lock(key).lock();
        }
    }

    /** Service that locks a key in {@link #execute()} and does not unlock it. */
    private static class LockService implements Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /** @param key Key. */
        LockService(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            ignite.cache(DEFAULT_CACHE_NAME).lock(key).lock();
        }
    }
}
