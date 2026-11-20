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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.verify.GridNotIdleException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partitionFileName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.POOL_NAME_IDLE_VERIFY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class NodeOrCacheStoppingDuringIdleVerifyTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS = 4;

    /** */
    private static final int KEYS = PARTS * 200;

    /** */
    private static final int NOT_IDLE_PART = 0;

    /** */
    private static volatile CountDownLatch partitionReadBlocked;

    /** */
    private static volatile CountDownLatch partitionReadUnblocked;

    /** */
    private static volatile CountDownLatch notIdlePartReadUnblocked;

    /** */
    @Parameterized.Parameter
    public boolean checkCrc;

    /** */
    private TestFailureHandler failureHnd;

    /** */
    @Parameterized.Parameters(name = "checkCrc={0}")
    public static Iterable<Object> parameters() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setFileIOFactory(new IdleVerifyThreadsBlockingFileIOFactory())
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setFailureHandler(failureHnd);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        failureHnd = new TestFailureHandler(false);

        partitionReadBlocked = new CountDownLatch(PARTS);
        partitionReadUnblocked = new CountDownLatch(1);
        notIdlePartReadUnblocked = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        unblockPartitionRead();

        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testWithCacheDestruction() throws Exception {
        prepareCluster();

        CacheGroupContext grp = grid(0).context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        IgniteInternalFuture<IdleVerifyResult> idleFut = startIdleVerify();

        IgniteInternalFuture<?> destroyFut = runAsync(() -> grid(0).destroyCache(DEFAULT_CACHE_NAME));

        assertTrue(waitForCondition(grp::isPreparedToStop, getTestTimeout()));

        assertFalse(waitForCondition(destroyFut::isDone, 1_000));

        unblockPartitionRead();

        destroyFut.get(getTestTimeout());

        assertFalse(F.isEmpty(idleFut.get(getTestTimeout()).exceptions()));
        assertNull(failureHnd.failureContext());
    }

    /** */
    @Test
    public void testWithDeactivation() throws Exception {
        prepareCluster();

        CacheGroupContext grp = grid(0).context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        IgniteInternalFuture<IdleVerifyResult> idleFut = startIdleVerify();

        IgniteInternalFuture<?> deactivateFut = runAsync(() -> grid(0).cluster().state(ClusterState.INACTIVE));

        assertTrue(waitForCondition(grp::isPreparedToStop, getTestTimeout()));

        assertFalse(waitForCondition(deactivateFut::isDone, 1_000));

        unblockPartitionRead();

        deactivateFut.get(getTestTimeout());

        assertFalse(F.isEmpty(idleFut.get(getTestTimeout()).exceptions()));

        grid(0).cluster().state(ClusterState.ACTIVE);

        assertEquals(KEYS, grid(0).cache(DEFAULT_CACHE_NAME).size());
        assertNull(failureHnd.failureContext());
    }

    /** */
    @Test
    public void testWithNodeStoppage() throws Exception {
        prepareCluster();

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        int key = partitionKeys(cache, NOT_IDLE_PART, 1, 0).get(0);

        cache.get(key);

        IgniteInternalFuture<IdleVerifyResult> idleFut = startIdleVerify();

        cache.put(key, -1);

        notIdlePartReadUnblocked.countDown();

        assertTrue(waitForCondition(
            () -> grid(0).context().pools().getIdleVerifyExecutorService().getCompletedTaskCount() > 0,
            getTestTimeout()
        ));

        GridKernalContext kctx = grid(0).context();

        IgniteInternalFuture<?> stopFut = runAsync(() -> stopGrid(0, true));

        assertTrue(waitForCondition(kctx::isStopping, getTestTimeout()));

        unblockPartitionRead();

        stopFut.get(getTestTimeout());

        IdleVerifyResult res = idleFut.get(getTestTimeout());

        assertTrue(X.hasCause(F.first(res.exceptions().values()), GridNotIdleException.class));
        assertNull(failureHnd.failureContext());
    }

    /** */
    private void prepareCluster() throws Exception {
        startGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        for (int k = 0; k < KEYS; k++)
            cache.put(k, k);

        stopGrid(0);

        startGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    private IgniteInternalFuture<IdleVerifyResult> startIdleVerify() throws Exception {
        IgniteInternalFuture<IdleVerifyResult> fut = runAsync(() -> idleVerify(grid(0), checkCrc, DEFAULT_CACHE_NAME));

        assertTrue(partitionReadBlocked.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        return fut;
    }

    /** */
    private static void unblockPartitionRead() {
        notIdlePartReadUnblocked.countDown();
        partitionReadUnblocked.countDown();
    }

    /** */
    private static class IdleVerifyThreadsBlockingFileIOFactory implements FileIOFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final FileIOFactory delegate = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new FileIODecorator(delegate.create(file, modes)) {
                @Override public int readFully(ByteBuffer destBuf, long position) throws IOException {
                    int res = super.readFully(destBuf, position);

                    if (Thread.currentThread().getName().startsWith(POOL_NAME_IDLE_VERIFY)) {
                        partitionReadBlocked.countDown();

                        U.awaitQuiet(file.getName().equals(partitionFileName(NOT_IDLE_PART))
                            ? notIdlePartReadUnblocked
                            : partitionReadUnblocked);
                    }

                    return res;
                }
            };
        }
    }
}
