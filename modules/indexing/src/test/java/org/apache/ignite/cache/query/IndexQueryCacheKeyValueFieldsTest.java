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

package org.apache.ignite.cache.query;

import java.util.LinkedHashMap;
import java.util.Random;
import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;

/** The test checks that IndexQuery correctly works with indexes contain single field: cache key or value. */
public class IndexQueryCacheKeyValueFieldsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final int CNT = 10_000;

    /** Whether to escape field names. */
    protected boolean escape() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(2);

        try (IgniteDataStreamer<String, Integer> streamer = crd.dataStreamer(CACHE)) {
            for (int i = 0; i < CNT; i++)
                streamer.addData(key(i), i);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        // Cache with signle value field.
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("f1", "java.lang.String");
        fields.put("f2", "java.lang.Integer");

        QueryEntity qryEntity = new QueryEntity()
            .setKeyFieldName("f1")
            .setValueFieldName("f2")
            .setTableName("TEST")
            .setKeyType(String.class.getName())
            .setValueType(Integer.class.getName())
            .setFields(fields)
            .setIndexes(F.asList(
                new QueryIndex("f1"),
                new QueryIndex("f2"),
                new QueryIndex("_VAL")));

        return cfg.setCacheConfiguration(
            new CacheConfiguration<String, Integer>(CACHE)
                .setSqlEscapeAll(escape())
                .setQueryEntities(F.asList(qryEntity))
            );
    }

    /** */
    @Test
    public void testValueAliasField() {
        check("f2", false);
    }

    /** */
    @Test
    public void testValueAliasUpperField() {
        if (escape())
            failOnEscape("F2", false);
        else
            check("F2", false);
    }

    /** */
    @Test
    public void testValueField() {
        check("_VAL", false);
    }

    /** */
    @Test
    public void testValueLowerField() {
        check("_val", false);
    }

    /** */
    @Test
    public void testKeyAliasField() {
        check("f1", true);
    }

    /** */
    @Test
    public void testKeyAliasUpperField() {
        if (escape())
            failOnEscape("F1", true);
        else
            check("F1", true);
    }

    /** */
    @Test
    public void testKeyField() {
        check("_KEY", true);
    }

    /** */
    @Test
    public void testKeyLowerField() {
        check("_key", true);
    }

    /** */
    private void check(String fld, boolean key) {
        int pivot = new Random().nextInt(CNT / 2);

        QueryCursor<Cache.Entry<String, Integer>> cursor = grid(0).cache(CACHE).query(
            new IndexQuery<String, Integer>(Integer.class)
                .setCriteria(gt(fld, key ? key(pivot) : pivot))
        );

        assertEquals(CNT - pivot - 1, cursor.getAll().size());
    }

    /** */
    private void failOnEscape(String fld, boolean key) {
        GridTestUtils.assertThrows(null, () -> {
            grid(0).cache(CACHE).query(
                new IndexQuery<String, Integer>(Integer.class).setCriteria(gt(fld, key ? key(0) : 0))
            ).getAll();
        }, IgniteException.class, "Failed to parse IndexQuery. No index found for criteria.");
    }

    /** */
    private static String key(int val) {
        return String.format("key_%1$5s", val).replace(' ', '0');
    }
}
