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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class PrecisionTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CACHE = "PERSON";

    /** */
    private static IgniteEx ignite;

    /** */
    private static final int KEY = 0;

    /** */
    private static final String VALID_STR = "01234";

    /** */
    private static final String INVALID_STR = "012345";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(PERSON_CACHE);
    }

    /** */
    @Test
    public void testInsertTableVarColumns() {
        checkCachePutInsert(startSqlPersonCache());
    }

    /** */
    @Test
    public void testInsertValueVarColumns() {
        checkCachePutInsert(startPersonCache());
    }

    /** */
    private void checkCachePutInsert(IgniteCache<Integer, Person> cache) {
        cache.put(KEY + 1, new Person(VALID_STR));
        assertNotNull(cache.get(KEY + 1));

        cache.put(KEY + 2, new Person(VALID_STR.getBytes(StandardCharsets.UTF_8)));
        assertNotNull(cache.get(KEY + 2));

        cache.query(sqlInsertQuery(KEY + 3, "str", VALID_STR));
        assertNotNull(cache.get(KEY + 3));

        cache.query(sqlInsertQuery(KEY + 4, "bin", VALID_STR.getBytes(StandardCharsets.UTF_8)));
        assertNotNull(cache.get(KEY + 4));

        assertPrecision(cache, () -> {
            cache.put(KEY, new Person(INVALID_STR));

            return null;
        }, "STR");

        assertPrecision(cache, () -> {
            cache.put(KEY, new Person(INVALID_STR.getBytes(StandardCharsets.UTF_8)));

            return null;
        }, "BIN");

        assertPrecision(cache, () -> {
            cache.query(sqlInsertQuery(KEY, "str", INVALID_STR));

            return null;
        }, "STR");

        assertPrecision(cache, () -> {
            cache.query(sqlInsertQuery(KEY, "bin", INVALID_STR.getBytes(StandardCharsets.UTF_8)));

            return null;
        }, "BIN");
    }

    /** */
    private void assertPrecision(IgniteCache cache, Callable<Object> clo, String colName) {
        GridTestUtils.assertThrows(null, clo, CacheException.class,
            "Value for a column '" + colName + "' is too long. Maximum length: 5, actual length: 6");

        assertNull(cache.get(KEY));
    }

    /** */
    private IgniteCache<Integer, Person> startPersonCache() {
        return ignite.createCache(new CacheConfiguration<Integer, Person>()
            .setName(PERSON_CACHE)
            .setQueryEntities(Collections.singletonList(personQueryEntity())));
    }

    /** */
    private IgniteCache startSqlPersonCache() {
        ignite.context().query().querySqlFields(new SqlFieldsQuery(
            "create table " + PERSON_CACHE + "(" +
            "   id int PRIMARY KEY," +
            "   str varchar(5)," +
            "   bin binary(5)" +
            ") with \"CACHE_NAME=" + PERSON_CACHE + ",VALUE_TYPE=" + Person.class.getName() + "\""), false);

        return ignite.cache(PERSON_CACHE);
    }

    /** */
    private SqlFieldsQuery sqlInsertQuery(int key, String field, Object val) {
        return new SqlFieldsQuery("insert into " + PERSON_CACHE + "(id, " + field + ") values (?, ?)")
            .setArgs(key, val);
    }

    /** */
    private QueryEntity personQueryEntity() {
        return new QueryEntity(Integer.class, Person.class)
            .setKeyType(Integer.class.getName())
            .setKeyFieldName("id");
    }

    /** */
    static class Person {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField(precision = 5)
        private final String str;

        /** */
        @QuerySqlField(precision = 5)
        private final byte[] bin;

        /** */
        Person(String str) {
            this.str = str;
            this.bin = null;
        }

        /** */
        Person(byte[] arr) {
            this.str = null;
            this.bin = arr;
        }
    }
}
