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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class CreateTableWithDateKeySelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODES_COUNT = 1;

    /** */
    private static Ignite ignite;

    /** */
    private static IgniteCache<?, ?> initCache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGridsMultiThreaded(NODES_COUNT, false);

        initCache = ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC")
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ignite = null;
        initCache = null;
    }

    /** */
    @Test
    public void testPassTableWithDateKeyCreation() {
        final String creationQry = "CREATE TABLE %s (id DATE primary key, dateField DATE) " +
            "WITH \"cache_name=%s, WRAP_VALUE=false\"";

        Map<java.sql.Date, java.sql.Date> ent = new HashMap<>();

        ent.put(java.sql.Date.valueOf("2018-09-01"), java.sql.Date.valueOf("2018-09-02"));

        ent.put(java.sql.Date.valueOf("2018-09-03"), java.sql.Date.valueOf("2018-09-04"));

        checkInsertUpdateDelete(creationQry, "Tab1", ent);
    }

    /** */
    @Test
    public void testPassTableWithTimeKeyCreation() {
        final String creationQry = "CREATE TABLE %s (id TIME primary key, dateField TIME) " +
            "WITH \"cache_name=%s, WRAP_VALUE=false\"";

        Map<Time, Time> ent = new HashMap<>();

        ent.put(Time.valueOf(LocalTime.now()), Time.valueOf(LocalTime.now().minusHours(1)));

        ent.put(Time.valueOf(LocalTime.now().minusHours(2)), Time.valueOf(LocalTime.now().minusHours(3)));

        checkInsertUpdateDelete(creationQry, "Tab2", ent);
    }

    /** */
    @Test
    public void testPassTableWithTimeStampKeyCreation() {
        final String creationQry = "CREATE TABLE %s (id TIMESTAMP primary key, dateField TIMESTAMP) " +
            "WITH \"cache_name=%s, WRAP_VALUE=false\"";

        Map<Timestamp, Timestamp> ent = new HashMap<>();

        ent.put(Timestamp.valueOf(LocalDateTime.now()), Timestamp.valueOf(LocalDateTime.now().minusHours(1)));

        ent.put(Timestamp.valueOf(LocalDateTime.now().minusHours(2)),
            Timestamp.valueOf(LocalDateTime.now().minusHours(3)));

        checkInsertUpdateDelete(creationQry, "Tab3", ent);
    }

    /**
     * Common testing logic
     *
     * @param creationQry Create table query.
     * @param tblName Table name.
     * @param entries Map with Key-Value pairs
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    private <K, V> void checkInsertUpdateDelete(
        final String creationQry,
        final String tblName,
        final Map<K, V> entries
    ) {
        final String cacheName = "TABLE_CACHE_" + tblName;

        try (FieldsQueryCursor<List<?>> cur = initCache.query(
            new SqlFieldsQuery(String.format(creationQry, tblName, cacheName)))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

        for (Map.Entry<K, V> e : entries.entrySet()) {
            try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
                "INSERT INTO " + tblName + " VALUES(?, ?)").setArgs(e.getKey(), e.getValue()))) {

                assertNotNull(cur);

                List<List<?>> rows = cur.getAll();

                assertEquals(1, rows.size());

                assertEquals(1L, rows.get(0).get(0));
            }
        }

        K key = null;
        V val = null;
        for (Map.Entry<K, V> e : entries.entrySet()) {
            assertSelection(tblName, cache, e.getKey(), e.getValue());

            assertEquals(e.getValue(), cache.get(e.getKey()));

            key = e.getKey();
            val = e.getValue();
        }

        cache.remove(key);

        assertAbsence(tblName, cache, key);

        assertFalse(cache.containsKey(key));

        cache.put(key, val);

        assertEquals(val, cache.get(key));

        assertSelection(tblName, cache, key, val);

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "DELETE FROM " + tblName + " WHERE id=?").setArgs(key))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(1L, rows.get(0).get(0));
        }

        assertAbsence(tblName, cache, key);

        assertFalse(cache.containsKey(key));
    }

    /**
     * Check absence of key in the table.
     *
     * @param tblName Table name.
     * @param cache Cache.
     * @param key Sample key.
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    private <K, V> void assertAbsence(final String tblName, final IgniteCache<K, V> cache, final K key) {
        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "select * from " + tblName + " where id=?").setArgs(key))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }
    }

    /**
     * Check presence of key in the table.
     *
     * @param tblName Table name.
     * @param cache Cache.
     * @param key Sample key.
     * @param val Sample value.
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    private <K, V> void assertSelection(final String tblName, final IgniteCache<K, V> cache, final K key,
        final V val) {
        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "select * from " + tblName + " where id=?").setArgs(key))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(key, rows.get(0).get(0));

            assertEquals(val, rows.get(0).get(1));
        }
    }
}
