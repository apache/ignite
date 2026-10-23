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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/** */
@RunWith(Parameterized.class)
public class IdleVerifyOrphanedTaskOnNodeStopTest extends GridCommonAbstractTest {
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
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
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

        // Take enough partitions to process.
        int parts = Math.max(Math.max(4, IgniteConfiguration.AVAILABLE_PROC_CNT) * 2, 16);
        int keys = parts * 100_000;

        // Create cache and fill some data.
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

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int keyUnderLoad = partitionKeys(cache, 0, 1, 0).get(0);

        AtomicBoolean stopUpdatingFlag = new AtomicBoolean();

        // Continuously updates one partition.
        IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(() -> {
            for (int i = 0; !stopUpdatingFlag.get(); i++)
                cache.put(keyUnderLoad, i);
        });

        // Expected to fail and correctly finish all its jobs because the cluster isn't idle.
        idleVerify(ignite, checkCrc, DEFAULT_CACHE_NAME);

        concurrentOp.run();

        stopUpdatingFlag.set(true);

        try {
            updateFut.get(getTestTimeout());
        }
        catch (Exception ignored) {
            // No-op.
        }
    }
}
