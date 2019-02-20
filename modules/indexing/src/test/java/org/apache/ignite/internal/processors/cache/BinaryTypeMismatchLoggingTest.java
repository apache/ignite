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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests of binary type mismatch logging.
 */
public class BinaryTypeMismatchLoggingTest extends GridCommonAbstractTest {
    /** */
    public static final String MESSAGE_PAYLOAD_VALUE = "expValType=Payload, actualValType=o.a.i.i.processors.cache.BinaryTypeMismatchLoggingTest$Payload";

    /** */
    private GridStringLogger capture;

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testValueReadCreateTable() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache def = ignite.createCache("default");

        def.query(new SqlFieldsQuery("CREATE TABLE binary (id INT PRIMARY KEY, str VARCHAR) " +
            "WITH \"cache_name=binary, value_type=Payload\"").setSchema("PUBLIC"));

        def.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (1, 'foo');").setSchema("PUBLIC"));
        def.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (2, 'bar');").setSchema("PUBLIC"));

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Object>() {
                @Override public Object call() {
                    return ignite.cache("binary").get(1);
                }
            },
            BinaryInvalidTypeException.class,
            "Payload");
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testValueReadQueryEntities() throws Exception {
        Ignite ignite = startGrid(0);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        IgniteCache binary = ignite.createCache(new CacheConfiguration<>()
            .setName("binary").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyFieldName("id").setValueType("Payload").setFields(fields).setTableName("binary"))));

        binary.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (1, 'foo');"));
        binary.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (2, 'bar');"));

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Object>() {
                @Override public Object call() {
                    return ignite.cache("binary").get(1);
                }
            },
            BinaryInvalidTypeException.class,
            "Payload");
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testEntryReadCreateTable() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache def = ignite.createCache("default");

        def.query(new SqlFieldsQuery("CREATE TABLE binary (id INT PRIMARY KEY, str VARCHAR) " +
            "WITH \"cache_name=binary, key_type=IdKey, value_type=Payload\"").setSchema("PUBLIC"));

        def.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (1, 'foo');").setSchema("PUBLIC"));
        def.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (2, 'bar');").setSchema("PUBLIC"));

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Object>() {
                @Override public Object call() {
                    return ignite.cache("binary").iterator().next();
                }
            },
            BinaryInvalidTypeException.class,
            "IdKey");
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testEntryReadQueryEntities() throws Exception {
        Ignite ignite = startGrid(0);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        IgniteCache binary = ignite.createCache(new CacheConfiguration<>()
            .setName("binary").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType("IdKey").setKeyFields(Collections.singleton("id"))
                .setValueType("Payload").setFields(fields).setTableName("binary"))));

        binary.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (1, 'foo');"));
        binary.query(new SqlFieldsQuery("INSERT INTO binary (id, str) VALUES (2, 'bar');"));

        GridTestUtils.assertThrowsAnyCause(
            log,
            new Callable<Object>() {
                @Override public Object call() {
                    return ignite.cache("binary").iterator().next();
                }
            },
            BinaryInvalidTypeException.class,
            "IdKey");
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testValueWriteCreateTable() throws Exception {
        Ignite ignite = startGridWithLogCapture();

        IgniteCache def = ignite.createCache("default");

        def.query(new SqlFieldsQuery("CREATE TABLE binary (id INT PRIMARY KEY, str VARCHAR) " +
            "WITH \"cache_name=binary, value_type=Payload\"").setSchema("PUBLIC"));

        IgniteCache<Integer, Payload> binary = ignite.cache("binary");
        binary.put(1, new Payload("foo"));
        binary.put(2, new Payload("bar"));

        assertEquals(0, countRows(binary));

        String capturedMessages = this.capture.toString();

        assertContainsExactlyOnce(capturedMessages,
            "Key-value pair is not inserted into any SQL table [cacheName=binary, " + MESSAGE_PAYLOAD_VALUE + "]");
        assertContainsExactlyOnce(capturedMessages,
            "Value type(s) are specified via CacheConfiguration.indexedTypes or CacheConfiguration.queryEntities");
        assertContainsExactlyOnce(capturedMessages,
            "Make sure that same type(s) used when adding Object or BinaryObject to cache");
        assertContainsExactlyOnce(capturedMessages,
            "Otherwise, entries will be stored in cache, but not appear as SQL Table rows");
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testValueWriteQueryEntities() throws Exception {
        Ignite ignite = startGridWithLogCapture();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        IgniteCache<Integer, Object> binary = ignite.createCache(new CacheConfiguration<Integer, Object>()
            .setName("binary").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyFieldName("id").setValueType("Payload").setFields(fields).setTableName("binary"))));

        binary.put(1, new Payload("foo"));
        binary.put(2, new IdKey(2));

        assertEquals(0, countRows(binary));

        assertContainsExactlyOnce(capture.toString(), MESSAGE_PAYLOAD_VALUE);
        assertContainsExactlyOnce(capture.toString(),
            "expValType=Payload, actualValType=o.a.i.i.processors.cache.BinaryTypeMismatchLoggingTest$IdKey");
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testEntryWriteCreateTable() throws Exception {
        Ignite ignite = startGridWithLogCapture();

        IgniteCache def = ignite.createCache("default");

        def.query(new SqlFieldsQuery("CREATE TABLE binary (id INT PRIMARY KEY, str VARCHAR) " +
            "WITH \"cache_name=binary, key_type=IdKey, value_type=Payload\"").setSchema("PUBLIC"));

        IgniteCache<Integer, Payload> binary = ignite.cache("binary");
        binary.put(1, new Payload("foo"));
        binary.put(2, new Payload("bar"));

        assertEquals(0, countRows(binary));

        assertContainsExactlyOnce(capture.toString(), MESSAGE_PAYLOAD_VALUE);

        capture.reset();

        def.query(new SqlFieldsQuery("CREATE TABLE binary2 (id INT PRIMARY KEY, str VARCHAR) " +
            "WITH \"cache_name=binary2, key_type=IdKey, value_type=Payload\"").setSchema("PUBLIC"));

        IgniteCache<Integer, Payload> binary2 = ignite.cache("binary2");
        binary2.put(1, new Payload("foo"));
        binary2.put(2, new Payload("bar"));

        assertEquals(0, countRows(binary2));

        assertContainsExactlyOnce(capture.toString(), MESSAGE_PAYLOAD_VALUE);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testEntryWriteQueryEntities() throws Exception {
        Ignite ignite = startGridWithLogCapture();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        IgniteCache<IdKey, Payload> binary = ignite.createCache(new CacheConfiguration<IdKey, Payload>()
            .setName("binary").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType("IdKey").setKeyFields(Collections.singleton("id"))
                .setValueType("Payload").setFields(fields).setTableName("binary"))));

        binary.put(new IdKey(1), new Payload("foo"));
        binary.put(new IdKey(2), new Payload("bar"));

        assertEquals(0, countRows(binary));

        binary.destroy();

        binary = ignite.createCache(new CacheConfiguration<IdKey, Payload>()
            .setName("binary").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType("IdKey").setKeyFields(Collections.singleton("id"))
                .setValueType("Payload").setFields(fields).setTableName("binary"))));

        binary.put(new IdKey(1), new Payload("foo"));
        binary.put(new IdKey(2), new Payload("bar"));

        assertEquals(0, countRows(binary));

        assertContainsExactlyOnce(capture.toString(), MESSAGE_PAYLOAD_VALUE);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testEntryWriteCacheIsolation() throws Exception {
        Ignite ignite = startGridWithLogCapture();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        IgniteCache<IdKey, Payload> regular = ignite.createCache(new CacheConfiguration<IdKey, Payload>()
            .setName("regular").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType(getClass().getName() + "$IdKey").setKeyFields(Collections.singleton("id"))
                .setValueType(getClass().getName() + "$Payload").setFields(fields).setTableName("binary"))));

        IgniteCache<IdKey, Payload> binary = ignite.createCache(new CacheConfiguration<IdKey, Payload>()
            .setName("binary").setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType("IdKey").setKeyFields(Collections.singleton("id"))
                .setValueType("Payload").setFields(fields).setTableName("binary"))));

        regular.put(new IdKey(1), new Payload("foo"));
        regular.put(new IdKey(2), new Payload("bar"));

        binary.put(new IdKey(1), new Payload("foo"));
        binary.put(new IdKey(2), new Payload("bar"));

        assertEquals(0, countRows(binary));
        assertEquals(2, countRows(regular));

        assertContainsExactlyOnce(capture.toString(), MESSAGE_PAYLOAD_VALUE);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testValueWriteMultipleQueryEntities() throws Exception {
        Ignite ignite = startGridWithLogCapture();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        IgniteCache<Integer, Payload> binary = ignite.createCache(new CacheConfiguration<Integer, Payload>()
            .setName("binary").setQueryEntities(Arrays.asList(
            new QueryEntity().setKeyType("Foo").setKeyFieldName("id")
                .setValueType("Bar").setFields(fields).setTableName("regular"),
            new QueryEntity().setKeyFieldName("id").setValueType("Payload").setFields(fields).setTableName("binary"))));

        binary.put(1, new Payload("foo"));
        binary.put(2, new Payload("bar"));

        assertEquals(0, countRows(binary));

        assertContainsExactlyOnce(capture.toString(),
            "valType=o.a.i.i.processors.cache.BinaryTypeMismatchLoggingTest$Payload");
    }

    /** */
    private <K> int countRows(IgniteCache<K, ?> binary) {
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT COUNT(*) FROM binary");

        List<List<?>> result = binary.query(qry).getAll();
        return Number.class.cast(result.get(0).get(0)).intValue();
    }

    /** */
    private void assertContainsExactlyOnce(String capturedMessages, String message) {
        assertTrue(capturedMessages.contains(message));

        assertEquals(-1, capturedMessages.indexOf(message, capturedMessages.indexOf(message) + 1));
    }

    /** */
    private IgniteEx startGridWithLogCapture() throws Exception {
        IgniteEx ignite = startGrid(0);

        this.capture = new GridStringLogger(false, this.log);

        GridTestUtils.setFieldValue(ignite.context().query(), GridProcessorAdapter.class,"log", capture);

        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    private static class IdKey {
        /** */
        @QuerySqlField
        private final int id;

        /** */
        public IdKey(int id) {
            this.id = id;
        }
    }

    /** */
    private static class Payload {
        /** */
        @QuerySqlField
        private final String str;

        /** */
        public Payload(String str) {
            this.str = str;
        }
    }
}
