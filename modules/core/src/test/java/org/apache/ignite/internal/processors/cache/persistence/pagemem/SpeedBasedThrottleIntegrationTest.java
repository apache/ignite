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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowCheckpointMetadataFileIOFactory;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Integration tests for {@link PagesWriteSpeedBasedThrottle}.
 */
public class SpeedBasedThrottleIntegrationTest extends GridCommonAbstractTest {
    /***/
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                // set small region size to make it easy achieve the necessity to throttle with speed-based throttle
                .setMaxSize(60 * 1024 * 1024)
                .setPersistenceEnabled(true)
            )
            .setCheckpointFrequency(200)
            .setWriteThrottlingEnabled(true)
            .setFileIOFactory(
                new SlowCheckpointMetadataFileIOFactory(
                    new AtomicBoolean(true), TimeUnit.MILLISECONDS.toNanos(10000)
                )
            );

        return cfg.setDataStorageConfiguration(dbCfg)
            .setConsistentId(gridName)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60 * 1000;
    }

    /**
     */
    @Test
    public void speedBasedThrottleShouldBeActivatedWhenNeeded() throws Exception {
        AtomicBoolean throttled = new AtomicBoolean(false);
        listeningLog.registerListener(message -> {
            if (message.startsWith("Throttling is applied to page modifications")) {
                throttled.set(true);
            }
        });

        Ignite ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);
        IgniteCache<Object, Object> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1_000_000; i++) {
            cache.put("key" + i, ThreadLocalRandom.current().nextDouble());

            if (throttled.get()) {
                break;
            }
        }

        assertTrue("Throttling was not triggered", throttled.get());
    }
}
