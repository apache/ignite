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

package org.apache.ignite.internal.processors.cache.objects;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class CacheObjectsCompressionMemoryConsumptionTest extends AbstractCacheObjectsCompressionTest {
    /** Huge string. */
    private static final String HUGE_STRING;

    /** Region name. */
    private static final String REGION_NAME = "region";

    static {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 1000; i++)
            sb.append("AAAAAAAAAA");

        HUGE_STRING = sb.toString();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setName(REGION_NAME)
                        .setMetricsEnabled(true)
                        .setMaxSize(1000L * 1024 * 1024)
                        .setInitialSize(1000L * 1024 * 1024))
                .setMetricsEnabled(true));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testString() throws Exception {
        testMemoryConsumption((i) -> i, (i) -> HUGE_STRING + i);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testWrapperString() throws Exception {
        testMemoryConsumption((i) -> i, (i) -> new StringData(HUGE_STRING + i));
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testIncompressible() {
        GridTestUtils.assertThrowsWithCause(
            () -> {
                testMemoryConsumption((i) -> i, (i) -> i);

                return null;
            }, AssertionError.class);
    }

    /**
     * @throws Exception If failed.
     */
    private void testMemoryConsumption(Function<Integer, Object> keyGen, Function<Integer, Object> valGen) throws Exception {
        List<Integer> cnts = new ArrayList<>();
        List<Long> raws = new ArrayList<>();
        List<Long> comps = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            int cnt = ThreadLocalRandom.current().nextInt(1_000, 2_000);
            long raw;
            long compressed;

            boolean reversed = i % 2 == 0;

            Function<Integer, Object> kGen = reversed ? valGen : keyGen;
            Function<Integer, Object> vGen = reversed ? keyGen : valGen;

            compressed = doTest(cnt, kGen, vGen);

            try {
                ZstdCompressionTransformer.fail = true;

                raw = doTest(cnt, kGen, vGen);
            }
            finally {
                ZstdCompressionTransformer.fail = false;
            }

            assertTrue("raw=" + raw + ", compressed=" + compressed, raw > compressed);

            cnts.add(cnt);
            raws.add(raw);
            comps.add(compressed);
        }

        log.info("Comparision result:\n cnt=" + cnts + "\n raw=" + raws + "\n compressed=" + comps);
    }

    /**
     *
     */
    private long doTest(int cnt, Function<Integer, Object> keyGen, Function<Integer, Object> valGen) throws Exception {
        try {
            Ignite ignite = startGrid();

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

            for (int i = 0; i < cnt; i++) {
                Object key = keyGen.apply(i);
                Object val = valGen.apply(i);

                cache.put(key, val);

                assertEquals(cache.get(key), val);
            }

            DataRegionMetrics metrics = ignite.dataRegionMetrics(REGION_NAME);

            return Math.round(metrics.getTotalUsedSize() * metrics.getPagesFillFactor());
        }
        finally {
            stopAllGrids();
        }
    }
}
