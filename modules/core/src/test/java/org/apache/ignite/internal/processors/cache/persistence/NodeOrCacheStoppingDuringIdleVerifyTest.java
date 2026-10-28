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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
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
    private static final Set<Long> blockedThreads = ConcurrentHashMap.newKeySet();

    /** */
    private static CountDownLatch blockedLatch;

    /**
     * Whether to check the CRC sums, see:
     * {@link IdleVerifyUtility#checkPartitionsPageCrcSum(IgniteThrowableSupplier, int, byte, BooleanSupplier)}.
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

        dsCfg.setFileIOFactory(new BlockedIdleVerifyFileIOFactory(dsCfg.getFileIOFactory()));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(dsCfg)
            .setFailureHandler(new StopNodeOrHaltFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        blockedThreads.clear();
        blockedLatch = new CountDownLatch(1);
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
        assertTrue(waitForCondition(() -> blockedThreads.size() == DFLT_IDLE_VERIFY_POOL_SIZE, getTestTimeout()));

        // Concurrent updates may stop the idle verify with the 'cache-not-idle' exception.
        AtomicBoolean stopUpdatingFlag = new AtomicBoolean(false);
        IgniteInternalFuture<?> updateFut = launchConcurrentUpdates(loadCli, stopUpdatingFlag);

        CacheGroupContext grp = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        IgniteInternalFuture<?> destroyCacheFut = runAsync(() -> grid(0).destroyCache(DEFAULT_CACHE_NAME));

        assertTrue(waitForCondition(grp::isPreparedToStop, 10_000L));

        blockedLatch.countDown();

        // The idle verify is expected to finish with cancellation, node stoppage, group stoppage or 'cache-not-idle' exception.
        assertFalse(F.isEmpty(idleFut.get().exceptions()));

        stopUpdatingFlag.set(true);

        destroyCacheFut.get(10_000L);

        // The cache updating cannot stop normally due to the cache destruction. This is ok.
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
    @Test
    public void testOrphanedIdleVerifyTaskOnNodeStoppage() throws Exception {
        prepareCluster();

        IgniteEx loadCli = startClientGrid();

        IgniteInternalFuture<IdleVerifyResult> idleFut = runAsync(() -> idleVerify(grid(0), checkCrc, DEFAULT_CACHE_NAME));

        // Wait until all the idle verify threads blocked.
        assertTrue(waitForCondition(() -> blockedThreads.size() == DFLT_IDLE_VERIFY_POOL_SIZE, getTestTimeout()));

        // Concurrent updates may stop the idle verify with the 'cache-not-idle' exception.
        AtomicBoolean stopUpdatingFlag = new AtomicBoolean(false);
        IgniteInternalFuture<?> updateFut = launchConcurrentUpdates(loadCli, stopUpdatingFlag);

        GridKernalContext kctx = grid(0).context();

        IgniteInternalFuture<?> stopNodeFut = runAsync(() -> stopGrid(0));

        assertTrue(waitForCondition(kctx::isStopping, 10_000L));

        blockedLatch.countDown();

        // The idle verify is expected to finish with cancellation, node stoppage, group stoppage or 'cache-not-idle' exception.
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
    private IgniteInternalFuture<?> launchConcurrentUpdates(IgniteEx ig, AtomicBoolean stopUpdatingFlag) {
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        // Continuously changes the partition update counter.
        IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(() -> {
            for (int i = 0; !stopUpdatingFlag.get(); i++)
                cache.put(i, i);
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

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int k = 0; k < keys; k++)
                streamer.addData(k, k);
        }

        // Restart for force keeping data on disk.
        stopGrid(0);
        startGrid(0);
    }

    /** */
    private static final class BlockedIdleVerifyFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private BlockedIdleVerifyFileIOFactory(FileIOFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                @Override public int read(ByteBuffer destBuf) throws IOException {
                    slowDownAndBlockIfRequired();

                    return super.read(destBuf);
                }

                @Override public int read(ByteBuffer destBuf, long position) throws IOException {
                    slowDownAndBlockIfRequired();

                    return super.read(destBuf, position);
                }

                @Override public int read(byte[] buf, int off, int len) throws IOException {
                    slowDownAndBlockIfRequired();

                    return super.read(buf, off, len);
                }

                @Override public int readFully(ByteBuffer destBuf) throws IOException {
                    slowDownAndBlockIfRequired();

                    return super.readFully(destBuf);
                }

                @Override public int readFully(ByteBuffer destBuf, long position) throws IOException {
                    slowDownAndBlockIfRequired();

                    return super.readFully(destBuf, position);
                }

                @Override public int readFully(byte[] buf, int off, int len) throws IOException {
                    slowDownAndBlockIfRequired();

                    return super.readFully(buf, off, len);
                }
            };
        }

        /** */
        private void slowDownAndBlockIfRequired() {
            if (!Thread.currentThread().getName().startsWith("idleVerify-#"))
                return;

            blockedThreads.add(Thread.currentThread().getId());

            if (log.isDebugEnabled())
                log.debug("Blocked thread: " + Thread.currentThread().getId() + " / " + Thread.currentThread().getName());

            try {
                blockedLatch.await(25_000L, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                // Proceed with the simultaneously delayed thread. Let it stop in its logic.
                Thread.currentThread().interrupt();
            }

            if (log.isDebugEnabled())
                log.debug("Released thread: " + Thread.currentThread().getId() + " / " + Thread.currentThread().getName());
        }
    }
}
