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

package org.apache.ignite.internal.processors.query.h2;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Tests queries against JSR-310 Java 8 Date and Time API keys and values.
 */
public class CacheQueryJsr310Java8DateTimeApiKeyAndValueTest extends CacheQueryJsr310Java8DateTimeApiBaseTest {
    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGridsMultiThreaded(1, true);
    }

    /**
     * Tests that the inserted key-value pair is stored as is.
     *
     * @param key Key.
     * @param val Value.
     * @param <K> Key type.
     * @param <V> Value type
     */
    private <K, V> void doTestInsertedKeyValuePairStoredAsIs(K key, V val) {
        A.notNull(key, "key");
        A.notNull(val, "val");

        final Class<?> keyCls = key.getClass();
        final Class<?> valCls = val.getClass();

        IgniteCache<K, V> cache =
            ignite.getOrCreateCache(createCacheConfig(valCls.getSimpleName() + "Cache", keyCls, valCls));

        SqlFieldsQuery qry =
            new SqlFieldsQuery("insert into " + valCls.getSimpleName() + "(_key, _val) values(?, ?)").setArgs(key, val);

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(1L, qryResults.get(0).get(0));
        assertEquals(1, cache.size());

        CacheEntry<K, V> entry = cache.getEntry(key);
        List<Cache.Entry<Object, Object>> all = cache.query(new ScanQuery<>()).getAll();

        assertNotNull(entry);
        assertEquals(key, entry.getKey());
        assertEquals(val, entry.getValue());
        assertEquals(keyCls, entry.getKey().getClass());
        assertEquals(valCls, entry.getValue().getClass());
    }

    /**
     * Tests that selection by a key-value pair returns the expected key and value types.
     *
     * @param key Key.
     * @param val Value.
     * @param expKeyCls Expected key class.
     * @param expValCls Expected value class.
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private <K, V> void doTestSelectByKeyValuePairReturnsExpectedKeyAndValueTypes(
        K key, V val, Class<?> expKeyCls, Class<?> expValCls
    ) {
        A.notNull(key, "key");
        A.notNull(val, "val");

        final Class<?> keyCls = key.getClass();
        final Class<?> valCls = val.getClass();

        IgniteCache<K, V> cache =
            ignite.getOrCreateCache(createCacheConfig(valCls.getSimpleName() + "Cache", keyCls, valCls));

        cache.put(key, val);

        SqlFieldsQuery qry =
            new SqlFieldsQuery("select _key, _val from " + valCls.getSimpleName() + " where _key=? and _val=?")
                .setArgs(key, val);

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(expKeyCls, qryResults.get(0).get(0).getClass());
        assertEquals(expValCls, qryResults.get(0).get(1).getClass());
    }

    /**
     * Tests updating a value by a key-value pair.
     *
     * @param key Key.
     * @param val Value.
     * @param newVal New value.
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private <K, V> void doTestUpdateValueByKeyValuePair(K key, V val, V newVal) {
        A.notNull(key, "key");
        A.notNull(val, "val");
        A.notNull(newVal, "newVal");

        final Class<?> keyCls = key.getClass();
        final Class<?> valCls = val.getClass();

        IgniteCache<K, V> cache =
            ignite.getOrCreateCache(createCacheConfig(valCls.getSimpleName() + "Cache", keyCls, valCls));

        cache.put(key, val);

        SqlFieldsQuery qry =
            new SqlFieldsQuery("update " + valCls.getSimpleName() + " set _val=? where _key=? and _val=?")
                .setArgs(newVal, key, val);

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(1L, qryResults.get(0).get(0));
        assertEquals(newVal, cache.get(key));
    }

    /**
     * Tests deleting by a key-value pair.
     *
     * @param key Key.
     * @param val Value.
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private <K, V> void doTestDeleteByKeyValuePair(K key, V val) {
        A.notNull(key, "key");
        A.notNull(val, "val");

        final Class<?> keyCls = key.getClass();
        final Class<?> valCls = val.getClass();

        IgniteCache<K, V> cache =
            ignite.getOrCreateCache(createCacheConfig(valCls.getSimpleName() + "Cache", keyCls, valCls));

        cache.put(key, val);

        SqlFieldsQuery qry =new SqlFieldsQuery("delete from " + valCls.getSimpleName() + " where _key=? and _val=?")
            .setArgs(key, val);

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(1L, qryResults.get(0).get(0));
        assertEquals(0, cache.size());
    }

    /**
     * Tests that the inserted {@link LocalTime} key-value pair is stored as is.
     *
     * @throws Exception If failed.
     */
    public void testInsertedLocalTimeKeyValuePairStoredAsIs() throws Exception {
        doTestInsertedKeyValuePairStoredAsIs(LOCAL_TIME, LOCAL_TIME);
    }

    /**
     * Tests that the inserted {@link LocalDate} key-value pair is stored as is.
     *
     * @throws Exception If failed.
     */
    public void testInsertedLocalDateKeyValuePairStoredAsIs() throws Exception {
        doTestInsertedKeyValuePairStoredAsIs(LOCAL_DATE, LOCAL_DATE);
    }

    /**
     * Tests that the inserted {@link LocalDateTime} key-value pair is stored as is.
     *
     * @throws Exception If failed.
     */
    public void testInsertedLocalDateTimeKeyValuePairStoredAsIs() throws Exception {
        doTestInsertedKeyValuePairStoredAsIs(LOCAL_DATE_TIME, LOCAL_DATE_TIME);
    }

    /**
     * Tests that selection by a {@link LocalTime} key-value pair returns {@link LocalTime} key and value types.
     *
     * @throws Exception If failed.
     */
    public void testSelectByLocalTimeKeyValuePairReturnsLocalTimeKeyAndValue() throws Exception {
        doTestSelectByKeyValuePairReturnsExpectedKeyAndValueTypes(
            LOCAL_TIME, LOCAL_TIME, LocalTime.class, LocalTime.class
        );
    }

    /**
     * Tests that selection by a {@link LocalDate} key-value pair returns {@link LocalDate} key and value types.
     *
     * @throws Exception If failed.
     */
    public void testSelectByLocalDateKeyValuePairReturnsLocalDateKeyAndValue() throws Exception {
        doTestSelectByKeyValuePairReturnsExpectedKeyAndValueTypes(
            LOCAL_DATE, LOCAL_DATE, LocalDate.class, LocalDate.class
        );
    }

    /**
     * Tests that selection by a {@link LocalDateTime} key-value pair returns {@link LocalDateTime} key and value types.
     *
     * @throws Exception If failed.
     */
    public void testSelectByLocalDateTimeKeyValuePairReturnsLocalDateTimeKeyAndValue() throws Exception {
        doTestSelectByKeyValuePairReturnsExpectedKeyAndValueTypes(
            LOCAL_DATE_TIME, LOCAL_DATE_TIME, LocalDateTime.class, LocalDateTime.class
        );
    }

    /**
     * Tests updating a value by a {@link LocalTime} key-value pair.
     *
     * @throws Exception If failed.
     */
    public void testUpdateValueByLocalTimeKeyValuePair() throws Exception {
        doTestUpdateValueByKeyValuePair(LOCAL_TIME, LOCAL_TIME, LOCAL_TIME.plusHours(1));
    }

    /**
     * Tests updating a value by a {@link LocalDate} key-value pair.
     *
     * @throws Exception If failed.
     */
    public void testUpdateValueByLocalDateKeyValuePair() throws Exception {
        doTestUpdateValueByKeyValuePair(LOCAL_DATE, LOCAL_DATE, LOCAL_DATE.plusDays(1));
    }

    /**
     * Tests updating a value by a {@link LocalDateTime} key-value pair.
     *
     * @throws Exception If failed.
     */
    public void testUpdateValueByLocalDateTimeKeyValuePair() throws Exception {
        doTestUpdateValueByKeyValuePair(LOCAL_DATE_TIME, LOCAL_DATE_TIME, LOCAL_DATE_TIME.plusHours(1));
    }

    /**
     * Test deleting by a {@link LocalTime} key-value pair.
     *
     * @throws Exception If failed.
     */
    public void testDeleteByLocalTimeKeyValuePair() throws Exception {
        doTestDeleteByKeyValuePair(LOCAL_TIME, LOCAL_TIME);
    }

    /**
     * Test deleting by a {@link LocalDate} key-value pair.
     *
     * @throws Exception If failed.
     */
    public void testDeleteByLocalDateKeyValuePair() throws Exception {
        doTestDeleteByKeyValuePair(LOCAL_DATE, LOCAL_DATE);
    }

    /**
     * Test deleting by a {@link LocalDateTime} key-value pair.
     *
     * @throws Exception If failed.
     */
    public void testDeleteByLocalDateTimeKeyValuePair() throws Exception {
        doTestDeleteByKeyValuePair(LOCAL_DATE_TIME, LOCAL_DATE_TIME);
    }
}
