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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.internal.util.typedef.F;
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
public class IdleVerifyOrphanedTaskOnNodeStopTest extends GridCommonAbstractTest {
    /** */
    private static final Map<Long, CountDownLatch> delayedThreads = new ConcurrentHashMap<>();

    /** */
    private static final Set<Long> resealedThreads = ConcurrentHashMap.newKeySet();

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
            new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(100L * 1024 * 1024L));

        dsCfg.setFileIOFactory(new SlowIdleVerifyFileIOFactory(dsCfg.getFileIOFactory()));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(dsCfg)
            .setFailureHandler(new StopNodeOrHaltFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testOrphanedIdleVerifyTaskOnNodeStoppage() throws Exception {
        doTestPageMemoryStoppageDuringIdleVerify(() -> stopGrid(0));
    }

    /** */
    @Test
    public void testOrphanedIdleVerifyTaskOnCacheDestruction() throws Exception {
        doTestPageMemoryStoppageDuringIdleVerify(() -> grid(0).destroyCache(DEFAULT_CACHE_NAME));
    }

    /** */
    private void doTestPageMemoryStoppageDuringIdleVerify(Runnable concurrentOp) throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        preloadCluster(ignite);

        // Restart for force keeping data on disk.
        stopGrid(0);
        startGrid(0);

        IgniteInternalFuture<IdleVerifyResult> idleFut = runAsync(() -> idleVerify(grid(0), checkCrc, DEFAULT_CACHE_NAME));

        // Wait until all the idle verify threads blocked.
        assertTrue(waitForCondition(() -> delayedThreads.size() == DFLT_IDLE_VERIFY_POOL_SIZE, getTestTimeout()));

        idleFut.cancel();

        // Release one idle verify thread.
        delayedThreads.remove(F.first(delayedThreads.keySet())).countDown();
        assertTrue(waitForCondition(() -> resealedThreads.size() == 1, getTestTimeout()));

        IdleVerifyResult idleRes = idleFut.get();

        assertFalse(idleRes.exceptions().isEmpty());

        concurrentOp.run();

        delayedThreads.values().forEach(CountDownLatch::countDown);
        delayedThreads.clear();

        // Wait until all the idle verify threads released.
        assertTrue(waitForCondition(() -> resealedThreads.size() == DFLT_IDLE_VERIFY_POOL_SIZE, getTestTimeout()));

        stopGrid(0);
    }

    /** */
    private static void preloadCluster(IgniteEx ignite) {
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
    }

    /** */
    private static final class SlowIdleVerifyFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private SlowIdleVerifyFileIOFactory(FileIOFactory delegateFactory) {
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
            if (!Thread.currentThread().getName().contains("idleVerify"))
                return;

            CountDownLatch latch = new CountDownLatch(1);

            delayedThreads.put(Thread.currentThread().getId(), latch);

            if (log.isDebugEnabled())
                log.debug("Blocked thread: " + Thread.currentThread().getId() + " / " + Thread.currentThread().getName());

            try {
                latch.await(15_000L, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                // Proceed with the simultaneously delayed thread. Let it stop it its logic.
                Thread.currentThread().interrupt();
            }

            resealedThreads.add(Thread.currentThread().getId());

            if (log.isDebugEnabled())
                log.debug("Released thread: " + Thread.currentThread().getId() + " / " + Thread.currentThread().getName());
        }
    }
}
