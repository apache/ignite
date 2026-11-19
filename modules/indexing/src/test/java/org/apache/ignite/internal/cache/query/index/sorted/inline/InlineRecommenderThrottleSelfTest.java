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
     * An insert of a key that differs from the stored keys in the very first inlined byte is decided within
     * the inline prefix and never reaches the recommender; an update of an existing key invokes it twice.
     * The {@code KEYS} updates therefore produce {@code 2 * KEYS} invocations in total, of which every
     * second one (threshold of 2) performs a check, so {@code KEYS} warnings are expected.
     */
    private final LogListener lsnr = LogListener.matches("cannot be fully inlined").times(KEYS).build();

    /**
     * The first performed check happens on the update of the shortest key, so the first warning must carry
     * that key's size (1000 UTF-8 bytes + 3 system bytes of a String inline value). Pins the invocation
     * order: if inserts reached the recommender as well, the first warning would be logged for a longer key.
     */
    private final LogListener firstKeyLsnr = LogListener.matches("recommendedInlineSize=1003]").times(1).build();

    /** */
    @Rule
    public final TestRule testRule = new SystemPropertiesRule();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        testLog.registerListener(lsnr);
        testLog.registerListener(firstKeyLsnr);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        lsnr.reset();
        firstKeyLsnr.reset();
    }

    /**
     * With {@code IGNITE_THROTTLE_INLINE_SIZE_CALCULATION=2} the recommendation must be calculated on every
     * second invocation of the recommender. Each of the {@code KEYS} keys is put twice, but only the second
     * put (an update of an existing key) reaches the recommender, and it does so twice, producing
     * {@code 2 * KEYS} invocations in total; exactly {@code KEYS} checks must therefore be performed.
     * Every check sees a strictly longer key than the previous one and must log a warning, so the first
     * warning must carry the shortest key's size; that pins the invocation order and is asserted below.
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

        // The keys differ in their first byte, so an insert is always decided within the inlined prefix
        // and only the updates reach the recommender. The keys get strictly longer so that every performed
        // check observes a new maximum size and logs a warning.
        for (int i = 0; i < KEYS; i++) {
            String key = (char)('a' + i) + "x".repeat(999 + i * 100);

            cache.put(key, new Value(i));
            cache.put(key, new Value(i));
        }

        assertTrue("With a throttle threshold of 2 the recommendation must be calculated for every second "
            + "qualifying put (" + KEYS + " of " + 2 * KEYS + " expected)", lsnr.check());

        assertTrue("The first warning must be logged for the shortest key (recommendedInlineSize=1003), "
            + "otherwise the invocation order differs from the documented one", firstKeyLsnr.check());
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
