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
    private static final String SQL_PERSON_CACHE = "SQL_PERSON";

    /** */
    private static IgniteEx ignite;

    /** */
    private static final int KEY = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache0(PERSON_CACHE, false);
        ignite.destroyCache0(SQL_PERSON_CACHE, true);
    }

    /** */
    @Test
    public void testInsertTableVarColumns() {
        IgniteCache tblCache = startSqlPersonCache();

        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into " + SQL_PERSON_CACHE + "(id, str) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into " + SQL_PERSON_CACHE + "(id, bin) values (?, ?)");

        String invalidStr = "123456";
        int len = invalidStr.length();

        assertPrecision(tblCache, () -> {
                tblCache.query(strQry.setArgs(KEY, invalidStr));

                return null;
            }, "STR", len);

        assertPrecision(tblCache, () -> {
                tblCache.query(binQry.setArgs(KEY, invalidStr.getBytes(StandardCharsets.UTF_8)));

                return null;
            }, "BIN", len);

        assertPrecision(tblCache, () -> {
                BinaryObject bo = ignite.binary().builder(SQL_PERSON_CACHE)
                    .setField("id", KEY)
                    .setField("str", invalidStr)
                    .build();

                tblCache.put(KEY, bo);

                return null;
            }, "STR", len);

        assertPrecision(tblCache, () -> {
                BinaryObject bo = ignite.binary().builder(SQL_PERSON_CACHE)
                    .setField("id", KEY)
                    .setField("bin", invalidStr.getBytes(StandardCharsets.UTF_8))
                    .build();

                tblCache.put(KEY, bo);

                return null;
        }, "BIN", len);
    }

    /** */
    @Test
    public void testInsertValueVarColumns() {
        IgniteCache<Integer, Person> cache = startPersonCache();

        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into PERSON(_KEY, name) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into PERSON(_KEY, arr) values (?, ?)");

        String invalidStr = "123456";
        int len = invalidStr.length();

        assertPrecision(cache, () -> {
                cache.put(KEY, new Person(invalidStr));

                return null;
            }, "NAME", len);

        assertPrecision(cache, () -> {
                cache.put(KEY, new Person(invalidStr.getBytes(StandardCharsets.UTF_8)));

                return null;
            }, "ARR", len);

        assertPrecision(cache, () -> {
                cache.query(strQry.setArgs(KEY, invalidStr));

                return null;
            }, "NAME", len);

        assertPrecision(cache, () -> {
                cache.query(binQry.setArgs(KEY, invalidStr.getBytes(StandardCharsets.UTF_8)));

                return null;
            }, "ARR", len);
    }

    /** */
    @Test
    public void testClientInsertTableVarColumns() throws Exception {
        IgniteCache tblCache = startSqlPersonCache();

        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into " + SQL_PERSON_CACHE + "(id, str) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into " + SQL_PERSON_CACHE + "(id, bin) values (?, ?)");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            ClientCache<Integer, Object> cc = client.cache(SQL_PERSON_CACHE);

            String invalidStr = "123456";
            int len = invalidStr.length();

            assertClientPrecision(tblCache, () -> {
                    cc.query(strQry.setArgs(KEY, invalidStr)).getAll();

                    return null;
                }, "STR", len);

            assertClientPrecision(tblCache, () -> {
                    cc.query(binQry.setArgs(KEY, invalidStr.getBytes(StandardCharsets.UTF_8))).getAll();

                    return null;
                }, "BIN", len);

            assertClientPrecision(tblCache, () -> {
                    BinaryObject bo = ignite(0).binary().builder(SQL_PERSON_CACHE)
                        .setField("id", KEY)
                        .setField("str", invalidStr)
                        .build();

                    cc.put(0, bo);

                    return null;
                }, "STR", len);

            assertClientPrecision(tblCache, () -> {
                    BinaryObject bo = ignite(0).binary().builder(SQL_PERSON_CACHE)
                        .setField("id", KEY)
                        .setField("bin", invalidStr.getBytes(StandardCharsets.UTF_8))
                        .build();

                    cc.put(0, bo);

                    return null;
                }, "BIN", len);
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
        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into PERSON(_KEY, name) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into PERSON(_KEY, arr) values (?, ?)");

        String invalidStr = "123456";
        int len = invalidStr.length();

        assertClientPrecision(cache, () -> {
            cc.put(KEY, new Person(invalidStr));

            return null;
        }, "NAME", len);

        assertClientPrecision(cache, () -> {
            cc.put(KEY, new Person(invalidStr.getBytes(StandardCharsets.UTF_8)));

            return null;
        }, "ARR", len);

        assertClientPrecision(cache, () -> {
            cc.query(strQry.setArgs(KEY, invalidStr)).getAll();

            return null;
        }, "NAME", len);

        assertClientPrecision(cache, () -> {
            cc.query(binQry.setArgs(KEY, invalidStr.getBytes(StandardCharsets.UTF_8))).getAll();

            return null;
        }, "ARR", len);
    }

    /** */
    private void assertPrecision(IgniteCache cache, Callable<Object> clo, String colName, int len) {
        GridTestUtils.assertThrows(null, clo, CacheException.class,
            "Value for a column '" + colName + "' is too long. Maximum length: 5, actual length: " + len);

        assertNull(cache.get(0));
    }

    /** */
    private void assertClientPrecision(IgniteCache cache, Callable<Object> clo, String colName, int len) {
        GridTestUtils.assertThrows(null, clo, ClientException.class,
            "Value for a column '" + colName + "' is too long. Maximum length: 5, actual length: " + len);

        assertNull(cache.get(0));
    }

    /** */
    private ClientCache startClientPersonCache(IgniteClient client) {
        QueryEntity queryEntity = new QueryEntity(Integer.class, Person.class);

        return client.createCache(new ClientCacheConfiguration()
            .setName(PERSON_CACHE)
            .setQueryEntities(queryEntity));
    }

    /** */
    private IgniteCache<Integer, Person> startPersonCache() {
        return ignite.createCache(new CacheConfiguration<Integer, Person>(PERSON_CACHE)
            .setIndexedTypes(Integer.class, Person.class));
    }

    /** */
    private IgniteCache startSqlPersonCache() {
        ignite.context().query().querySqlFields(new SqlFieldsQuery("" +
            "create table " + SQL_PERSON_CACHE + "(" +
            "   id int PRIMARY KEY," +
            "   str varchar(5)," +
            "   bin binary(5)" +
            ") with \"CACHE_NAME=" + SQL_PERSON_CACHE + ",VALUE_TYPE=" + SQL_PERSON_CACHE + "\""), false);

        return ignite.cache(SQL_PERSON_CACHE);
    }

    /** */
    static class Person {
        /** */
        @QuerySqlField(precision = 5)
        private final String name;

        /** */
        @QuerySqlField(precision = 5)
        private final byte[] arr;

        /** */
        Person(String name) {
            this.name = name;
            this.arr = null;
        }

        /** */
        Person(byte[] arr) {
            this.name = null;
            this.arr = arr;
        }
    }
}
