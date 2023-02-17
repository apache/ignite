/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Test TTL worker with persistence enabled
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "5")
public class IgnitePdsWithTtlTest3 extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME_ATOMIC = "expirable-cache-atomic";

    /** */
    public static final int PART_SIZE = 1024;

    /** */
    private static final int EXPIRATION_TIMEOUT = 5_000;

    /** */
    public static final int ENTRIES = 50_000;

    /** */
    private static final int WORKLOAD_THREADS_CNT = 8;

    /** Fail. */
    private volatile boolean failureHndTriggered;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration dfltRegion = new DataRegionConfiguration()
            .setMaxSize(2L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegmentSize(16 * 1024 * 1024)
                .setMaxWalArchiveSize(256 * 1024 * 1024)
                .setCheckpointFrequency(10_000)
                .setDefaultDataRegionConfiguration(dfltRegion)
                .setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(
            getCacheConfiguration(CACHE_NAME_ATOMIC).setAtomicityMode(ATOMIC)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failureHndTriggered = true;

                return super.handle(ignite, failureCtx);
            }
        };
    }

    /**
     * Returns a new cache configuration with the given name and {@code GROUP_NAME} group.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> getCacheConfiguration(String name) {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>();

        ccfg.setName(name);
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PART_SIZE));
        ccfg.setEagerTtl(true);

        return ccfg;
    }

    /**
     * Check the node start after deactiovation with TTL cache.
     */
    @Test
    public void testStartAfterDeactivateWithTtlExpiring() throws Exception {
        try {
            IgniteEx srv = startGrid(0);

            srv.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, String> cache = srv.cache(CACHE_NAME_ATOMIC);

            AtomicBoolean timeoutReached = new AtomicBoolean(false);

            AtomicInteger threadId = new AtomicInteger(0);

            IgniteInternalFuture<?> ldrFut = runMultiThreadedAsync(() -> {
                int id = threadId.getAndIncrement();

                int i = 0;
                while (!timeoutReached.get()) {
                    cache.put(id * 1_000_000 + i, "datadatadatadatadaat");
                    i++;
                }
            }, WORKLOAD_THREADS_CNT, "loader");

            doSleep(EXPIRATION_TIMEOUT * 2);
            timeoutReached.set(true);
            ldrFut.get();

            printStatistics((IgniteCacheProxy)cache, "After cache puts");

            doSleep(2_000);

            srv.cluster().state(INACTIVE);

            doSleep(5_000);
            stopGrid(0);

            startGrid(0);
            doSleep(10_000);
        }
        finally {
            stopAllGrids();

            assertFalse("Failure handler should not be triggered.", failureHndTriggered);
        }
    }

    /**
     *
     */
    private void printStatistics(IgniteCacheProxy cache, String msg) {
        System.out.println(msg + " {{");
        cache.context().printMemoryStats();
        System.out.println("}} " + msg);
    }
}
