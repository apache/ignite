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

package org.apache.ignite.failure;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests critical failure handling on checkpoint read lock acquisition errors.
 */
public class CheckpointReadLockFailureTest extends GridCommonAbstractTest {
    /** */
    private static final AbstractFailureHandler FAILURE_HND = new AbstractFailureHandler() {
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            if (hndLatch != null && failureCtx.type() == FailureType.SYSTEM_WORKER_BLOCKED)
                hndLatch.countDown();

            return false;
        }
    };

    /** */
    private static final long CHECKPOINT_FREQ = 5_000;

    /** */
    private static volatile CountDownLatch hndLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(FAILURE_HND)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(CHECKPOINT_FREQ)
                .setCheckpointReadLockTimeout(1));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailureTypeOnTimeout() throws Exception {
        hndLatch = new CountDownLatch(1);

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        AtomicBoolean stopLoad = new AtomicBoolean();

        IgniteInternalFuture<Long> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            try {
                while (!stopLoad.get()) {
                    cache.put(rnd.nextInt(), rnd.nextInt());

                    doSleep(10);
                }
            }
            catch (Exception e) {
                // No-op.
            }
        }, 4, "cache-load");

        ignite.context().cache().context().database().wakeupForCheckpoint("test");

        hndLatch.await();

        stopLoad.set(true);

        loadFut.get(1, TimeUnit.SECONDS);

        stopAllGrids();
    }
}
