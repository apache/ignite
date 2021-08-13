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
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
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
        IgniteCache tblCache = startSqlPersonCache();

        assertPrecision(tblCache, () -> {
                tblCache.query(sqlInsertQuery("str"));

                return null;
            }, "STR");

        assertPrecision(tblCache, () -> {
                tblCache.query(sqlInsertQuery("bin"));

                return null;
            }, "BIN");

        assertPrecision(tblCache, () -> {
                tblCache.put(KEY, personBinaryObject("str"));

                return null;
            }, "STR");

        assertPrecision(tblCache, () -> {
                tblCache.put(KEY, personBinaryObject("bin"));

                return null;
        }, "BIN");
    }

    /** */
    @Test
    public void testInsertValueVarColumns() {
        IgniteCache<Integer, Person> cache = startPersonCache();

        assertPrecision(cache, () -> {
                cache.put(KEY, new Person(INVALID_STR));

                return null;
            }, "STR");

        assertPrecision(cache, () -> {
                cache.put(KEY, new Person(INVALID_STR.getBytes(StandardCharsets.UTF_8)));

                return null;
            }, "BIN");

        assertPrecision(cache, () -> {
                cache.query(sqlInsertQuery("str"));

                return null;
            }, "STR");

        assertPrecision(cache, () -> {
                cache.query(sqlInsertQuery("bin"));

                return null;
            }, "BIN");
    }

    /** */
    @Test
    public void testClientInsertTableVarColumns() throws Exception {
        IgniteCache tblCache = startSqlPersonCache();

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            ClientCache<Integer, Object> cc = client.cache(PERSON_CACHE);

            assertClientPrecision(tblCache, () -> {
                    cc.query(sqlInsertQuery("str")).getAll();

                    return null;
                }, "STR");

            assertClientPrecision(tblCache, () -> {
                    cc.query(sqlInsertQuery("bin")).getAll();

                    return null;
                }, "BIN");

            assertClientPrecision(tblCache, () -> {
                    cc.put(0, personBinaryObject("str"));

                    return null;
                }, "STR");

            assertClientPrecision(tblCache, () -> {
                    cc.put(0, personBinaryObject("bin"));

                    return null;
                }, "BIN");
        }
    }

    /** */
    @Test
    public void testClientInsertValueVarColumns() throws Exception {
        IgniteCache<Integer, Person> cache = startPersonCache();

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            ClientCache<Integer, Person> cc = client.cache(PERSON_CACHE);

            checkFromClient(cache, cc);
        }
    }

    /** */
    @Test
    public void testClientCreatedCache() throws Exception {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            ClientCache cc = startClientPersonCache(client);

            IgniteCache cache = ignite.cache(PERSON_CACHE);

            checkFromClient(cache, cc);
        }
    }

    /** */
    private void checkFromClient(IgniteCache cache, ClientCache cc) {
        assertClientPrecision(cache, () -> {
            cc.put(KEY, new Person(INVALID_STR));

            return null;
        }, "STR");

        assertClientPrecision(cache, () -> {
            cc.put(KEY, new Person(INVALID_STR.getBytes(StandardCharsets.UTF_8)));

            return null;
        }, "BIN");

        assertClientPrecision(cache, () -> {
            cc.query(sqlInsertQuery("str")).getAll();

            return null;
        }, "STR");

        assertClientPrecision(cache, () -> {
            cc.query(sqlInsertQuery("bin")).getAll();

            return null;
        }, "BIN");
    }

    /** */
    private void assertPrecision(IgniteCache cache, Callable<Object> clo, Class<? extends Throwable> exCls, String colName) {
        GridTestUtils.assertThrows(null, clo, exCls,
            "Value for a column '" + colName + "' is too long. Maximum length: 5, actual length: 6");

        assertNull(cache.get(KEY));
    }

    /** */
    private void assertPrecision(IgniteCache cache, Callable<Object> clo, String colName) {
        assertPrecision(cache, clo, CacheException.class, colName);
    }

    /** */
    private void assertClientPrecision(IgniteCache cache, Callable<Object> clo, String colName) {
        assertPrecision(cache, clo, ClientException.class, colName);
    }

    /** */
    private ClientCache startClientPersonCache(IgniteClient client) {
        return client.createCache(new ClientCacheConfiguration()
            .setName(PERSON_CACHE)
            .setQueryEntities(personQueryEntity()));
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

    /** */
    private SqlFieldsQuery sqlInsertQuery(String field) {
        Object arg = "bin".equalsIgnoreCase(field) ? INVALID_STR.getBytes(StandardCharsets.UTF_8) : INVALID_STR;

        return new SqlFieldsQuery("insert into " + PERSON_CACHE + "(id, " + field + ") values (?, ?)")
            .setArgs(KEY, arg);
    }

    /** */
    private BinaryObject personBinaryObject(String field) {
        Object o = "bin".equalsIgnoreCase(field) ? INVALID_STR.getBytes(StandardCharsets.UTF_8) : INVALID_STR;

        return ignite.binary().builder(Person.class.getName())
            .setField("id", KEY)
            .setField(field, o)
            .build();
    }

    /** */
    private QueryEntity personQueryEntity() {
        return new QueryEntity(Integer.class, Person.class)
            .setKeyType(Integer.class.getName())
            .setKeyFieldName("id");
    }
}
