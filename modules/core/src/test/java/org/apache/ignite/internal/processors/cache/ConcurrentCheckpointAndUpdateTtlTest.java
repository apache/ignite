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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
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
    public boolean dataStreamer;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode mode;

    /** */
    @Parameterized.Parameters(name = "dataStreamer={0}, cacheMode={1}")
    public static Collection<Object[]> params() {
        Collection<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode mode: CacheAtomicityMode.values()) {
            params.add(new Object[] { false, mode });
            params.add(new Object[] { true, mode });
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

        runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                if (dataStreamer)
                    stream(cln);
                else
                    put(cln);
            }
        }, 1, "streamer");

        runMultiThreadedAsync(() -> {
            while (!stop.get())
                grid(0).cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).getAll();
        }, 1, "touch");

        runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                try {
                    forceCheckpoint(F.asList(grid(0), grid(1)));
                }
                catch (Exception e) {
                    // No-op.
                }
            }
        }, 1, "checkpoint");

        Thread.sleep(60_000);

        stop.set(true);
    }

    /** */
    private void put(Ignite cln) {
        cln.cache(DEFAULT_CACHE_NAME).put(KEY, rnd.nextInt());
    }

    /** */
    private void stream(Ignite cln) {
        try (IgniteDataStreamer<Integer, Integer> stream = cln.dataStreamer(DEFAULT_CACHE_NAME)) {
            // Skip annoying warnings.
            stream.allowOverwrite(true);

            Map<Integer, Integer> map = new HashMap<>();

            map.put(KEY, rnd.nextInt());

            stream.addData(map);
            stream.flush();
        }
    }
}
