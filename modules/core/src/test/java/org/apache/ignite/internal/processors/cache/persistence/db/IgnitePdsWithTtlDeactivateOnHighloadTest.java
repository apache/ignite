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

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test TTL worker with persistence enabled
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "5")
public class IgnitePdsWithTtlDeactivateOnHighloadTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "expirable-cache";

    /** */
    private static final String GROUP_NAME = "group1";

    /** */
    private static final int PART_SIZE = 2;

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    private static final int ENTRIES = 10;

    /** */
    private static final int WORKLOAD_THREADS_CNT = 8;

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

        cfg.setCacheConfiguration(getCacheConfiguration(CACHE_NAME));

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
    private CacheConfiguration getCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(GROUP_NAME);
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
    public void shouldNotBeProblemToPutInExpiredCacheConcurrently() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        final IgniteEx srv = startGrid(0);

        srv.cluster().active(true);

        // Start high workload
        IgniteInternalFuture loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!end.get() && !fail) {
                IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

                for (int i = 0; i < ENTRIES; i++)
                    cache.put(i, new byte[1024]);

                //Touch entries.
                for (int i = 0; i < ENTRIES; i++)
                    cache.get(i); // touch entries
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

    /** */
    protected void fillCache(IgniteCache<Integer, byte[]> cache) {
        for (int i = 0; i < ENTRIES; i++)
            cache.put(i, new byte[1024]);

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries
    }
}

