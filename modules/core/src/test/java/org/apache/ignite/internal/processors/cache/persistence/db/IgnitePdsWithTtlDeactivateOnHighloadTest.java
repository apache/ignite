package org.apache.ignite.internal.processors.cache.persistence.db;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForAllFutures;

/**
 * Test TTL worker with persistence enabled
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "5")
public class IgnitePdsWithTtlDeactivateOnHighloadTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME_ATOMIC = "expirable-cache-atomic";

    /** */
    private static final String CACHE_NAME_TX = "expirable-cache-tx";

    /** */
    private static final String CACHE_NAME_LOCAL_ATOMIC = "expirable-cache-local-atomic";

    /** */
    private static final String CACHE_NAME_LOCAL_TX = "expirable-cache-local-tx";

    /** */
    private static final String CACHE_NAME_NEAR_ATOMIC = "expirable-cache-near-atomic";

    /** */
    private static final String CACHE_NAME_NEAR_TX = "expirable-cache-near-tx";

    /** */
    private static final int PART_SIZE = 2;

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    private static final int ENTRIES = 10;

    /** */
    private static final int WORKLOAD_THREADS_CNT = 16;

    /** Fail. */
    private volatile boolean fail;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);

        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(2L * 1024 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ).setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(
            getCacheConfiguration(CACHE_NAME_ATOMIC).setAtomicityMode(ATOMIC),
            getCacheConfiguration(CACHE_NAME_TX).setAtomicityMode(TRANSACTIONAL),
            getCacheConfiguration(CACHE_NAME_LOCAL_ATOMIC).setAtomicityMode(ATOMIC).setCacheMode(CacheMode.LOCAL),
            getCacheConfiguration(CACHE_NAME_LOCAL_TX).setAtomicityMode(TRANSACTIONAL).setCacheMode(CacheMode.LOCAL),
            getCacheConfiguration(CACHE_NAME_NEAR_ATOMIC).setAtomicityMode(ATOMIC)
                .setNearConfiguration(new NearCacheConfiguration<>()),
            getCacheConfiguration(CACHE_NAME_NEAR_TX).setAtomicityMode(TRANSACTIONAL)
                .setNearConfiguration(new NearCacheConfiguration<>())
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                fail = true;

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
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PART_SIZE));
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void shouldNotBeProblemToPutToExpiredCacheConcurrentlyWithCheckpoint() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ig0.getOrCreateCache(CACHE_NAME_ATOMIC);

        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();

        IgniteInternalFuture<?> ldrFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                Map<Object, Object> map = new TreeMap<>();

                for (int i = 0; i < ENTRIES; i++)
                    map.put(i, i);

                cache.putAll(map);
            }
        }, WORKLOAD_THREADS_CNT, "loader");

        IgniteInternalFuture<?> updaterFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                for (int i = 0; i < ENTRIES; i++)
                    cache.put(i, i * 10);
            }
        }, WORKLOAD_THREADS_CNT, "updater");

        IgniteInternalFuture<?> cpWriteLockUnlockFut = runAsync(() -> {
            ReentrantReadWriteLock lock = U.field(db, "checkpointLock");

            while (!timeoutReached.get()) {
                try {
                    lock.writeLock().lockInterruptibly();

                    doSleep(30);
                }
                catch (InterruptedException ignored) {
                    break;
                }
                finally {
                    lock.writeLock().unlock();
                }

                doSleep(30);
            }
        }, "cp-write-lock-holder");

        doSleep(10_000);

        timeoutReached.set(true);

        waitForAllFutures(cpWriteLockUnlockFut, ldrFut, updaterFut);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void shouldNotBeProblemToPutToExpiredCacheConcurrently() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        final IgniteEx srv = startGrids(3);

        srv.cluster().state(ClusterState.ACTIVE);

        // Start high workload.
        IgniteInternalFuture<?> loadFut = runMultiThreadedAsync(() -> {
            List<IgniteCache<Object, Object>> caches = F.asList(
                srv.cache(CACHE_NAME_ATOMIC),
                srv.cache(CACHE_NAME_TX),
                srv.cache(CACHE_NAME_LOCAL_ATOMIC),
                srv.cache(CACHE_NAME_LOCAL_TX),
                srv.cache(CACHE_NAME_NEAR_ATOMIC),
                srv.cache(CACHE_NAME_NEAR_TX)
            );

            while (!end.get() && !fail) {
                for (IgniteCache<Object, Object> cache : caches) {
                    for (int i = 0; i < ENTRIES; i++)
                        cache.put(i, new byte[1024]);

                    cache.putAll(new TreeMap<>(F.asMap(0, new byte[1024], 1, new byte[1024])));

                    for (int i = 0; i < ENTRIES; i++)
                        cache.get(i);

                    cache.getAll(new TreeSet<>(F.asList(0, 1)));
                }
            }
        }, WORKLOAD_THREADS_CNT, "high-workload");

        try {
            // Let's wait some time.
            loadFut.get(10, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            assertFalse("Failure handler was called. See log above.", fail);

            assertTrue(X.hasCause(e, IgniteFutureTimeoutCheckedException.class));
        }
        finally {
            end.set(true);
        }

        assertFalse("Failure handler was called. See log above.", fail);
    }
}
