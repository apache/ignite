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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CreateTableWithDateKeySelfTest extends GridCommonAbstractTest {
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
        stopAllGrids();
    }

    /** */
    public void testPassTableWithDateKeyCreation() {
        final String creationQry = "CREATE TABLE %s (id DATE primary key, dateField DATE) " +
            "WITH \"cache_name=%s, WRAP_VALUE=false\"";

        final Date key = new Date();

        final Date val = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));

        checkInsertUpdateDelete(creationQry, "Tab1", key, val);
    }

    /** */
    public void testPassTableWithTimeKeyCreation() {
        final String creationQry = "CREATE TABLE %s (id TIME primary key, dateField TIME) " +
            "WITH \"cache_name=%s, WRAP_VALUE=false\"";

        final Time key = Time.valueOf(LocalTime.now());

        final Time val = Time.valueOf(LocalTime.now());

        checkInsertUpdateDelete(creationQry, "Tab2", key, val);
    }

    /** */
    public void testPassTableWithTimeStampKeyCreation() {
        final String creationQry = "CREATE TABLE %s (id TIMESTAMP primary key, dateField TIMESTAMP) " +
            "WITH \"cache_name=%s, WRAP_VALUE=false\"";

        final Timestamp key = Timestamp.valueOf(LocalDateTime.now());

        final Timestamp val = Timestamp.valueOf(LocalDateTime.now());

        checkInsertUpdateDelete(creationQry, "Tab3", key, val);
    }

    /**
     * Common testing logic
     *
     * @param creationQry Create table query.
     * @param tblName Table name.
     * @param key Sample key.
     * @param val Sample value.
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    private <K, V> void checkInsertUpdateDelete(
        final String creationQry,
        final String tblName,
        final K key,
        final V val) {
        final String cacheName = "TABLE_CACHE_" + tblName;

        try (FieldsQueryCursor<List<?>> cur = initCache.query(
            new SqlFieldsQuery(String.format(creationQry, tblName, cacheName)))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "INSERT INTO " + tblName + " VALUES(?, ?)").setArgs(key, val))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(1L, rows.get(0).get(0));
        }

        assertSelection(tblName, cache, key, val);

        assertEquals(val, cache.get(key));

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
