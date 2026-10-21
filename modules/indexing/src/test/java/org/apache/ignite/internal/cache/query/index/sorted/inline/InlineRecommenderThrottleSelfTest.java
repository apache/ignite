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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests that the inline size recommendation is calculated on every Nth invocation of
 * {@link InlineRecommender#recommend}, where N is {@code IGNITE_THROTTLE_INLINE_SIZE_CALCULATION}.
 */
public class InlineRecommenderThrottleSelfTest extends GridCommonAbstractTest {
    /** Number of keys to put; each key is inserted and updated once. */
    private static final int KEYS = 6;

    /**
     * The {@code 2 * KEYS} puts produce {@code 2 * KEYS} recommender invocations in total, of which
     * every second one (threshold of 2) performs a check, so {@code KEYS} warnings are expected.
     */
    private final LogListener lsnr = LogListener.matches("cannot be fully inlined").times(KEYS).build();

    /** */
    @Rule
    public final TestRule testRule = new SystemPropertiesRule();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        testLog.registerListener(lsnr);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        lsnr.reset();
    }

    /**
     * With {@code IGNITE_THROTTLE_INLINE_SIZE_CALCULATION=2} the recommendation must be calculated on every
     * second invocation of the recommender. The {@code KEYS} rows are put twice each (insert and update);
     * the puts of variable-length keys that exceed the inline size produce {@code 2 * KEYS} invocations
     * in total, so exactly {@code KEYS} checks must be performed; every check sees a strictly longer key
     * than the previous one and must therefore log a warning.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "2")
    public void testRecommendationCalculatedOnEveryNthPut() throws Exception {
        IgniteEx ignite = startGrid(0);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", String.class.getName());
        fields.put("val", Integer.class.getName());

        CacheConfiguration<String, Value> ccfg = new CacheConfiguration<String, Value>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType(String.class.getName())
                .setValueType(Value.class.getName())
                .setKeyFieldName("id")
                .setFields(fields)));

        IgniteCache<String, Value> cache = ignite.createCache(ccfg);

        // Keys get strictly longer so that every calculated size exceeds the previously recommended one
        // and each performed check logs a warning.
        for (int i = 0; i < KEYS; i++) {
            String key = (char)('a' + i) + "x".repeat(999 + i * 100);

            cache.put(key, new Value(i));
            cache.put(key, new Value(i));
        }

        assertTrue("With a throttle threshold of 2 the recommendation must be calculated for every second "
            + "qualifying put (" + KEYS + " of " + 2 * KEYS + " expected)", lsnr.check());
    }

    /** */
    private static class Value {
        /** */
        private final int val;

        /** */
        Value(int val) {
            this.val = val;
        }
    }
}
