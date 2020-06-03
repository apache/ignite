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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Test;

import javax.cache.CacheException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;

/**
 *
 */
public class SqlFieldTypeValidationOnKeyValueInsertTest extends AbstractIndexingCommonTest {
    /** */
    private static boolean validate;

    /** */
    private static final String SQL_TEXT = "select id, name from Person where id=1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setValidationEnabled(validate);

        cfg.setCacheConfiguration(new CacheConfiguration<>("testCache").setQueryEntities(
                Collections.singletonList(
                        new QueryEntity()
                                .setKeyFieldName("id")
                                .setValueType("Person")
                                .setFields(new LinkedHashMap<>(
                                        F.asMap("id", "java.lang.Integer",
                                                "name", "java.util.UUID"))))));

        cfg.setBinaryConfiguration(new BinaryConfiguration().setNameMapper(new BinaryBasicNameMapper(true)));

        return cfg;
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testError() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache("testCache");

        BinaryObject obj = grid(0).binary().builder("Person")
                             .setField("id", 1)
                             .setField("name", UUID.randomUUID().toString())
                             .build();

        GridTestUtils.assertThrows(log, () -> cache.put(1, obj), IgniteException.class,
                "Type for a column 'NAME' is not compatible with table definition.");

        assertNull(cache.withKeepBinary().get(1));
    }

    /** */
    @Test
    public void testSuccess() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache("testCache");

        BinaryObject obj = grid(0).binary().builder("Person")
                .setField("id", 1)
                .setField("name", UUID.randomUUID())
                .build();

        cache.put(1, obj);

        assertEquals(obj, cache.withKeepBinary().get(1));

        grid(0).context().query()
                .querySqlFields(
                new SqlFieldsQuery(SQL_TEXT).setSchema("testCache"), true)
                .getAll();
    }

    /** */
    @Test
    public void testSkipValidation() throws Exception {
        validate = false;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache("testCache");

        BinaryObject obj = grid(0).binary().builder("Person")
                .setField("id", 1)
                .setField("name", UUID.randomUUID().toString())
                .build();

        cache.put(1, obj);

        assertEquals(obj, cache.withKeepBinary().get(1));

        GridTestUtils.assertThrows(log, () ->
            grid(0).context().query().querySqlFields(new SqlFieldsQuery(SQL_TEXT).setSchema("testCache"), true).getAll(),
            CacheException.class,
            "java.lang.String cannot be cast to java.util.UUID");
    }

    /** */
    @Test
    public void testClassInstanceError() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache("testCache");

        GridTestUtils.assertThrows(log,
                () -> cache.put(1, new Person(UUID.randomUUID().toString())),
                IgniteException.class,
                "Type for a column 'NAME' is not compatible with table definition.");

        assertNull(cache.withKeepBinary().get(1));
    }

    /** */
    @Test
    public void testClassInstanceSkipValidation() throws Exception {
        validate = false;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache("testCache");

        Person obj = new Person(UUID.randomUUID().toString());

        cache.put(1, obj);

        assertEquals(obj, cache.get(1));

        GridTestUtils.assertThrows(log, () ->
            grid(0).context().query().querySqlFields(new SqlFieldsQuery(SQL_TEXT).setSchema("testCache"), true).getAll(),
            CacheException.class,
            "java.lang.String cannot be cast to java.util.UUID");
    }

    /** */
    public static class Person {
        private Object name;

        public Person(String name) {
            this.name = name;
        }

        public Object name() {
            return name;
        }

        @Override public boolean equals(Object obj) {
            return obj instanceof Person && F.eq(((Person) obj).name, name);
        }
    }
}
