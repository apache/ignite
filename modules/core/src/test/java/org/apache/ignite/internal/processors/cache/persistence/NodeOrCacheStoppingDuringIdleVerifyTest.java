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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.DFLT_IDLE_VERIFY_POOL_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class NodeOrCacheStoppingDuringIdleVerifyTest extends GridCommonAbstractTest {
    /** */
    private static final Set<Long> BLOCKED_THREADS = ConcurrentHashMap.newKeySet();

    /**
     * Flag to avoid threads reblocking if one file read(...) unexpectedly uses another read(...).
     *
     * @see IdleVerifyThreadsBlockingFileIOFactory
     */
    private static volatile boolean threadsBlockingAllowed;

    /**
     * Whether to check the CRC sums.
     *
     * @see IdleVerifyUtility#checkPartitionsPageCrcSum(IgniteThrowableSupplier, int, byte, BooleanSupplier)
     */
    @Parameterized.Parameter
    public boolean checkCrc;

    /** */
    @Parameterized.Parameters(name = "checkCrc={0}")
    public static Iterable<Object> parameters() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true));

        dsCfg.setFileIOFactory(new IdleVerifyThreadsBlockingFileIOFactory(dsCfg.getFileIOFactory()));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(dsCfg)
            .setFailureHandler(new StopNodeOrHaltFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        threadsBlockingAllowed = true;
        BLOCKED_THREADS.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testOrphanedIdleVerifyTaskOnCacheDestruction() throws Exception {
        prepareCluster();

        IgniteEx loadCli = startClientGrid();

        IgniteInternalFuture<IdleVerifyResult> idleFut = runAsync(() -> idleVerify(grid(0), checkCrc, DEFAULT_CACHE_NAME));

        // Wait until all the idle verify threads blocked.
        assertTrue(waitForCondition(() -> BLOCKED_THREADS.size() == DFLT_IDLE_VERIFY_POOL_SIZE, getTestTimeout()));

        // Concurrent updates may stop the idle verify with the 'cache-not-idle' exception.
        AtomicBoolean stopUpdatingFlag = new AtomicBoolean(false);
        IgniteInternalFuture<?> updateFut = launchConcurrentUpdates(loadCli, stopUpdatingFlag);

        CacheGroupContext grp = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        IgniteInternalFuture<?> destroyCacheFut = runAsync(() -> grid(0).destroyCache(DEFAULT_CACHE_NAME));

        assertTrue(waitForCondition(grp::isPreparedToStop, 10_000L));

        stopThreadsBlocking();

        // The idle verify is expected to finish with cancellation, node stoppage, group stoppage or the 'cache-not-idle' exception.
        assertFalse(F.isEmpty(idleFut.get().exceptions()));

        stopUpdatingFlag.set(true);

        destroyCacheFut.get();

        // The cache updating cannot stop normally due to the cache destruction. This is ok.
        try {
            updateFut.get();
        }
        catch (IgniteInterruptedCheckedException | IgniteFutureCancelledCheckedException e) {
            throw new IllegalStateException("Unexpected exception.", e);
        }
        catch (IgniteCheckedException ignored) {
            //no-op.
        }
    }

    /** */
    @Test
    public void testOrphanedIdleVerifyTaskOnNodeStoppage() throws Exception {
        prepareCluster();

        IgniteEx loadCli = startClientGrid();

        IgniteInternalFuture<IdleVerifyResult> idleFut = runAsync(() -> idleVerify(grid(0), checkCrc, DEFAULT_CACHE_NAME));

        // Wait until all the idle verify threads blocked.
        assertTrue(waitForCondition(() -> BLOCKED_THREADS.size() == DFLT_IDLE_VERIFY_POOL_SIZE, getTestTimeout()));


        // Concurrent updates may stop the idle verify with the 'cache-not-idle' exception.
        AtomicBoolean stopUpdatingFlag = new AtomicBoolean(false);
        IgniteInternalFuture<?> updateFut = launchConcurrentUpdates(loadCli, stopUpdatingFlag);

        GridKernalContext kctx = grid(0).context();

        IgniteInternalFuture<?> stopNodeFut = runAsync(() -> stopGrid(0));

        assertTrue(waitForCondition(kctx::isStopping, 10_000L));

        stopThreadsBlocking();

        // The idle verify is expected to finish with cancellation, node stoppage, group stoppage or the 'cache-not-idle' exception.
        assertFalse(F.isEmpty(idleFut.get().exceptions()));

        stopUpdatingFlag.set(true);

        stopNodeFut.get(10_000L);

        // The cache updating cannot stop normally due to the node stoppage. This is ok.
        try {
            updateFut.get(10_000L);
        }
        catch (IgniteInterruptedCheckedException | IgniteFutureTimeoutCheckedException | IgniteFutureCancelledCheckedException e) {
            throw new IllegalStateException("Unexpected exception.", e);
        }
        catch (IgniteCheckedException ignored) {
            //no-op.
        }
    }

    /** */
    private void stopThreadsBlocking() {
        threadsBlockingAllowed = false;

        synchronized (BLOCKED_THREADS) {
            BLOCKED_THREADS.notifyAll();
        }
    }

    /** */
    private IgniteInternalFuture<?> launchConcurrentUpdates(IgniteEx ig, AtomicBoolean stopUpdatingFlag) {
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        int keyUnderLoad = partitionKeys(cache, 0, 1, 0).get(0);

        // Continuously changes the partition update counter.
        IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(() -> {
            for (int i = 0; !stopUpdatingFlag.get(); i++)
                cache.put(keyUnderLoad, i);
        });

        return updateFut;
    }

    /** */
    private void prepareCluster() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        int parts = DFLT_IDLE_VERIFY_POOL_SIZE * 2;
        int keys = parts * 10;

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setAffinity(new RendezvousAffinityFunction(false, parts)));

        Affinity<Integer> aff = ignite.affinity(DEFAULT_CACHE_NAME);

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int k = 0; k < keys; k++) {
                if (aff.partition(k) != 0)
                    streamer.addData(k, k);
            }
        }

        // Restart for force keeping data on disk.
        stopGrid(0);
        startGrid(0);
    }

    /** */
    private static final class IdleVerifyThreadsBlockingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private IdleVerifyThreadsBlockingFileIOFactory(FileIOFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                @Override public int read(ByteBuffer destBuf) throws IOException {
                    blockIfRequired();

                    return super.read(destBuf);
                }

                @Override public int read(ByteBuffer destBuf, long position) throws IOException {
                    blockIfRequired();

                    return super.read(destBuf, position);
                }

                @Override public int read(byte[] buf, int off, int len) throws IOException {
                    blockIfRequired();

                    return super.read(buf, off, len);
                }
            };
        }

        /** */
        private void blockIfRequired() {
            if (!threadsBlockingAllowed || !Thread.currentThread().getName().startsWith(PoolProcessor.POOL_NAME_IDLE_VERIFY + "-#"))
                return;

            BLOCKED_THREADS.add(Thread.currentThread().getId());

            if (log.isDebugEnabled())
                log.error("Blocked thread: " + Thread.currentThread().getId() + " / " + Thread.currentThread().getName());

            try {
                synchronized (BLOCKED_THREADS) {
                    BLOCKED_THREADS.wait(15_000L);
                }
            }
            catch (InterruptedException e) {
                log.error("Unexpected interruption while waiting for the threads to release.", e);

                throw new IllegalStateException("Unexpected interruption.", e);
            }

            if (log.isDebugEnabled())
                log.error("Released thread: " + Thread.currentThread().getId() + " / " + Thread.currentThread().getName());
        }
    }
}
