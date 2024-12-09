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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/** */
@RunWith(Parameterized.class)
public class ConcurrentCheckpointAndUpdateTtlTest extends GridCommonAbstractTest {
    /** */
    private static final int KEY = 0;

    /** */
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    /** */
    @Parameterized.Parameter
    public boolean updateWithDataStreamer;

    /** */
    @Parameterized.Parameter(1)
    public boolean touchWithScanQuery;

    /** */
    @Parameterized.Parameter(2)
    public CacheAtomicityMode mode;

    /** */
    @Parameterized.Parameters(name = "dataStreamer={0}, touchQuery={1}, cacheMode={2}")
    public static Collection<Object[]> params() {
        Collection<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode cacheMode: CacheAtomicityMode.values()) {
            for (boolean updateMode: new boolean[] {true, false}) {
                for (boolean touchMode: new boolean[] {true, false})
                    params.add(new Object[] { updateMode, touchMode, cacheMode });
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)))
                .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(mode)
                    .setBackups(1)
                    .setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 10))));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void concurrentUpdateTouchCheckpointKey() throws Exception {
        startGrids(2).cluster().state(ClusterState.ACTIVE);

        IgniteEx cln = startClientGrid(2);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> updateFut = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                if (updateWithDataStreamer) {
                    // Skip annoying warnings.
                    Configurator.setLevel(DataStreamerImpl.class.getName(), org.apache.logging.log4j.Level.ERROR);

                    try (IgniteDataStreamer<Integer, Integer> stream = cln.dataStreamer(DEFAULT_CACHE_NAME)) {
                        stream.addData(KEY, rnd.nextInt());
                    }
                }
                else
                    cln.cache(DEFAULT_CACHE_NAME).put(KEY, rnd.nextInt());
            }
        }, 1, "update");

        IgniteInternalFuture<?> touchFut = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                if (touchWithScanQuery)
                    cln.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).getAll();
                else
                    cln.cache(DEFAULT_CACHE_NAME).get(KEY);
            }
        }, 1, "touch");

        IgniteInternalFuture<?> cpFut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < 1_000; i++) {
                try {
                    forceCheckpoint(F.asList(grid(0), grid(1)));
                }
                catch (Exception e) {
                    // No-op.
                }
            }

            stop.set(true);
        }, 1, "checkpoint");

        GridTestUtils.waitForAllFutures(updateFut, touchFut, cpFut);
    }
}
