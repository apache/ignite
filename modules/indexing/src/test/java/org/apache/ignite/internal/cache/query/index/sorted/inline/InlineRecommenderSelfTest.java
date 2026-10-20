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
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests that the inline size recommendation suggests both ways of increasing inline size
 * of a primary key index: the global system property and the per-cache
 * {@code CacheConfiguration.sqlIndexMaxInlineSize}.
 */
@WithSystemProperty(key = "IGNITE_THROTTLE_INLINE_SIZE_CALCULATION", value = "1")
public class InlineRecommenderSelfTest extends GridCommonAbstractTest {
    /** */
    @ClassRule
    public static final TestRule classRule = new SystemPropertiesRule();

    /** */
    private static final LogListener LSNR = LogListener.matches(msg ->
        msg.contains("IGNITE_MAX_INDEX_PAYLOAD_SIZE") && msg.contains("CacheConfiguration.sqlIndexMaxInlineSize")
    ).times(1).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        testLog.registerListener(LSNR);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        LSNR.reset();
    }

    /**
     * A row with a long indexed string key cannot be fully inlined into the primary key index;
     * the logged recommendation must mention both the {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} system property
     * and the per-cache {@code CacheConfiguration.sqlIndexMaxInlineSize}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecommendationMentionsSqlIndexMaxInlineSize() throws Exception {
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

        String key = "a".repeat(1024);

        // The first put into an empty index tree does not compare against an existing row,
        // and a differing key is decided within the inlined prefix, so the recommendation
        // is only triggered by an update of a row whose key exceeds the inline size.
        cache.put(key, new Value(1));
        cache.put(key, new Value(2));

        assertTrue("Inline size recommendation must mention both the system property and the per-cache "
            + "CacheConfiguration.sqlIndexMaxInlineSize option", LSNR.check());
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
