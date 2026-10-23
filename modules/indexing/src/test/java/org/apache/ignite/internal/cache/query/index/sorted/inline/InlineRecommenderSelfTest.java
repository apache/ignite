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
import org.apache.ignite.cache.QueryIndex;
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
 * Tests that the inline size recommendation mentions all the ways of increasing the inline size:
 * both the global system property and the per-cache {@code CacheConfiguration.sqlIndexMaxInlineSize}
 * for primary key indexes, and {@code QueryGroupIndex.inlineSize} for group indexes among
 * secondary index options.
 */
@WithSystemProperty(key = "IGNITE_THROTTLE_INLINE_SIZE_CALCULATION", value = "1")
public class InlineRecommenderSelfTest extends GridCommonAbstractTest {
    /** */
    @ClassRule
    public static final TestRule classRule = new SystemPropertiesRule();

    /** */
    private static final LogListener PK_IDX_LSNR = LogListener.matches(msg ->
        msg.contains("IGNITE_MAX_INDEX_PAYLOAD_SIZE") && msg.contains("CacheConfiguration.sqlIndexMaxInlineSize")
    ).times(1).build();

    /** */
    private static final LogListener GRP_IDX_LSNR = LogListener.matches("QueryGroupIndex.inlineSize")
        .times(1).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        testLog.registerListener(PK_IDX_LSNR);
        testLog.registerListener(GRP_IDX_LSNR);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        PK_IDX_LSNR.reset();
        GRP_IDX_LSNR.reset();
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
            + "CacheConfiguration.sqlIndexMaxInlineSize option", PK_IDX_LSNR.check());
    }

    /**
     * A row with a long indexed string column cannot be fully inlined into a secondary index;
     * the logged recommendation must mention {@code QueryGroupIndex.inlineSize} for group indexes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecommendationMentionsGroupIndexInlineSize() throws Exception {
        IgniteEx ignite = startGrid(0);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", Integer.class.getName());
        fields.put("str", String.class.getName());

        CacheConfiguration<Integer, Value> ccfg = new CacheConfiguration<Integer, Value>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(Value.class.getName())
                .setKeyFieldName("id")
                .setFields(fields)
                .setIndexes(Collections.singleton(new QueryIndex("str")))));

        IgniteCache<Integer, Value> cache = ignite.createCache(ccfg);

        Value val = new Value("a".repeat(1024));

        // The first put into an empty index tree does not compare against an existing row,
        // so the recommendation can only be triggered by a subsequent put.
        cache.put(1, val);
        cache.put(1, val);

        assertTrue("Inline size recommendation for secondary indexes must mention QueryGroupIndex.inlineSize "
            + "for group indexes", GRP_IDX_LSNR.check());
    }

    /** */
    private static class Value {
        /** */
        private final int val;

        /** */
        private final String str;

        /** */
        Value(int val) {
            this.val = val;

            this.str = null;
        }

        /** */
        Value(String str) {
            this.val = 0;

            this.str = str;
        }
    }
}
