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

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test correct clean up cache configuration data after destroying cache.
 */
public class IgnitePdsDestroyCacheTest extends IgnitePdsDestroyCacheAbstractTest {
    /**
     *  Test destroy non grouped caches.
     *
     *  @throws Exception If failed.
     */
    @Test
    public void testDestroyCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     *  Test destroy grouped caches.
     *
     *  @throws Exception If failed.
     */
    @Test
    public void testDestroyGroupCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     * Test destroy caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testDestroyCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Test destroy group caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testDestroyGroupCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Tests if a checkpoint is not blocked forever by concurrent cache destroying (DHT).
     */
    @Test
    public void testDestroyCacheOperationNotBlockingCheckpointTest() throws Exception {
        doTestDestroyCacheOperationNotBlockingCheckpointTest(false);
    }

    /**
     * Tests if a checkpoint is not blocked forever by concurrent cache destroying (local).
     */
    @Test
    public void testDestroyCacheOperationNotBlockingCheckpointTest_LocalCache() throws Exception {
        doTestDestroyCacheOperationNotBlockingCheckpointTest(true);
    }

    /**
     *
     */
    private void doTestDestroyCacheOperationNotBlockingCheckpointTest(boolean loc) throws Exception {
        final IgniteEx ignite = startGrids(1);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite, loc);

        loadCaches(ignite, !loc);

        // It's important to clear cache in group having > 1 caches.
        final String cacheName = cacheName(0);
        final CacheGroupContext grp = ignite.cachex(cacheName).context().group();

        final IgniteCacheOffheapManager offheap = grp.offheap();

        IgniteCacheOffheapManager mgr = Mockito.spy(offheap);

        final CountDownLatch checkpointLocked = new CountDownLatch(1);
        final CountDownLatch cpFutCreated = new CountDownLatch(1);
        final CountDownLatch realMtdCalled = new CountDownLatch(1);
        final CountDownLatch checked = new CountDownLatch(1);

        Mockito.doAnswer(invocation -> {
            checkpointLocked.countDown();

            assertTrue(U.await(cpFutCreated, 30, TimeUnit.SECONDS));

            Object ret = invocation.callRealMethod();

            // After calling clearing code cp future must be eventually completed and cp read lock reacquired.
            realMtdCalled.countDown();

            // Wait for checkpoint future while holding lock.
            U.awaitQuiet(checked);

            return ret;
        }).when(mgr).stopCache(Mockito.anyInt(), Mockito.anyBoolean());

        final Field field = U.findField(CacheGroupContext.class, "offheapMgr");
        field.set(grp, mgr);

        final IgniteInternalFuture<Object> fut = runAsync(() -> {
            assertTrue(U.await(checkpointLocked, 30, TimeUnit.SECONDS));

            // Trigger checkpoint while holding checkpoint read lock on cache destroy.
            final IgniteInternalFuture cpFut = ignite.context().cache().context().database().wakeupForCheckpoint("test");

            assertFalse(cpFut.isDone());

            cpFutCreated.countDown();

            assertTrue(U.await(realMtdCalled, 30, TimeUnit.SECONDS));

            try {
                cpFut.get(3_000); // Future must be completed after cache clearing but before releasing checkpoint lock.
            }
            finally {
                checked.countDown();
            }

            return null;
        });

        if (loc)
            ignite.cache(cacheName).close();
        else
            ignite.destroyCache(cacheName);

        fut.get();
    }
}
