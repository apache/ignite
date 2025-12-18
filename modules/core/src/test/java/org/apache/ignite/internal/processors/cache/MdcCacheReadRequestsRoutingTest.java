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

import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class MdcCacheReadRequestsRoutingTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** */
    private static final String KEY = "key";

    /** */
    private static final String VAL = "val";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        // Enforce different mac addresses to emulate distributed environment by default.
        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /** */
    @CartesianTest(name = "atomicity={0}, replicated={1}")
    public void testReadFromCache(
            @CartesianTest.Enum(value = CacheAtomicityMode.class) CacheAtomicityMode atomicityMode,
            @CartesianTest.Values(booleans = {true, false}) boolean replicated
    ) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean bool = rnd.nextBoolean();

        int nodes = 20;

        for (int i = 0; i < nodes; i += 2) {
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_0 : DC_ID_1);

            startGrid(i);

            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_1 : DC_ID_0);

            startGrid(i + 1);
        }

        awaitPartitionMapExchange();

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_0 : DC_ID_1);

        IgniteEx client = startClientGrid();

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);

        if (replicated)
            ccfg.setCacheMode(CacheMode.REPLICATED);
        else
            ccfg.setBackups(rnd.nextInt(nodes / 2 + 1 /*at least one in every DC*/, nodes /*less than all nodes*/));

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Object, Object> cache = client.createCache(ccfg);

        cache.put(KEY, VAL);

        for (int i = 0; i < nodes; i++) {
            if (Objects.equals(grid(i).localNode().dataCenterId(), bool ? DC_ID_1 : DC_ID_0)) {
                TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(i));

                spi.blockMessages((n, msg) -> true);
            }
        }

        assertEquals(VAL, cache.get(KEY));
    }
}
