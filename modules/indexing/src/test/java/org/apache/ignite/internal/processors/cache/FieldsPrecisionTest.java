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
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
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
public class FieldsPrecisionTest extends GridCommonAbstractTest {
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
        Stream.of("str", "bin").forEach(fld -> {
            Person validPerson = Person.newPerson(fld, true);

            cache.put(KEY, validPerson);
            assertEquals(validPerson, cache.get(KEY));

            cache.clear();

            cache.query(sqlInsertQuery(fld, VALID_STR));
            assertEquals(validPerson, cache.get(KEY));

            cache.clear();

            assertPrecision(cache, () -> {
                cache.put(KEY, Person.newPerson(fld, false));

                return null;
            }, fld.toUpperCase());

            assertPrecision(cache, () -> {
                cache.query(sqlInsertQuery(fld, INVALID_STR));

                return null;
            }, fld.toUpperCase());
        });
    }

    /** */
    private void assertPrecision(IgniteCache<Integer, Person> cache, Callable<Object> clo, String colName) {
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
    private IgniteCache<Integer, Person> startSqlPersonCache() {
        ignite.context().query().querySqlFields(new SqlFieldsQuery(
            "create table " + PERSON_CACHE + "(" +
            "   id int PRIMARY KEY," +
            "   str varchar(5)," +
            "   bin binary(5)" +
            ") with \"CACHE_NAME=" + PERSON_CACHE + ",VALUE_TYPE=" + Person.class.getName() + "\""), false);

        return ignite.cache(PERSON_CACHE);
    }

    /** */
    private SqlFieldsQuery sqlInsertQuery(String field, String s) {
        Object arg = "bin".equalsIgnoreCase(field) ? s.getBytes(StandardCharsets.UTF_8) : s;

        return new SqlFieldsQuery("insert into " + PERSON_CACHE + "(id, " + field + ") values (?, ?)")
            .setArgs(KEY, arg);
    }

    /** */
    private QueryEntity personQueryEntity() {
        QueryEntity entity = new QueryEntity(Integer.class, Person.class);
        entity.setKeyFieldName("id");
        entity.addQueryField("id", Integer.class.getName(), "ID");

        return entity;
    }

    /** */
    static class Person {
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

        /** */
        static Person newPerson(String fld, boolean valid) {
            String s = valid ? VALID_STR : INVALID_STR;

            return "bin".equals(fld) ? new Person(s.getBytes(StandardCharsets.UTF_8)) : new Person(s);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(str, person.str) && Arrays.equals(bin, person.bin);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(str);
            result = 31 * result + Arrays.hashCode(bin);
            return result;
        }
    }
}
