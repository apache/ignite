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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class CacheMdcGetTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** */
    private static final String KEY = "key";

    /** */
    private static final String VAL = "val";

    /** */
    @Parameterized.Parameters(name = "atomicity={0}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
            res.add(new Object[] {mode});
        }

        return res;
    }

    /** */
    @Parameterized.Parameter()
    public CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        // Enforce different mac adresses to emulate distributed environment by default.
        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean bool = rnd.nextBoolean();

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_0 : DC_ID_1);

        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, bool ? DC_ID_1 : DC_ID_0);

        startGrid(1);

        waitForTopology(2);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        IgniteEx client = startClientGrid();

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setBackups(1);
        ccfg.setAtomicityMode(atomicityMode);

        IgniteCache<Object, Object> cache = client.createCache(ccfg);

        cache.put(KEY, VAL);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(bool ? grid(0) : grid(1));

        spi.blockMessages((n, msg) -> true);

        assertEquals(VAL, cache.get(KEY));
    }
}
