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
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class PrecisionTest extends GridCommonAbstractTest {
    /** */
    private IgniteCache<Integer, Person> cache;

    /** */
    private static final int KEY = 0;

    /** */
    private static final int CNT = 10;


    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>("PERSON")
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = startGrids(2);

        cache = ignite.cache("PERSON");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testInsertTableVarColumns() {
        cache.query(new SqlFieldsQuery("" +
            "create table TABLE(" +
            "   id int PRIMARY KEY," +
            "   str varchar(5)," +
            "   bin binary(5)" +
            ") with \"CACHE_NAME=SQL,VALUE_TYPE=SQL_TYPE\""));

        StringBuilder validStr = new StringBuilder("12345");

        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into TABLE(id, str) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into TABLE(id, bin) values (?, ?)");

        IgniteCache tblCache = ignite(0).cache("SQL");

        for (int i = 1; i < CNT; i++) {
            validStr.append("0");

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    cache.query(strQry.setArgs(KEY, validStr.toString()));

                    return null;
                } }, "STR", 5 + i);

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    cache.query(binQry.setArgs(KEY, validStr.toString().getBytes(StandardCharsets.UTF_8)));

                    return null;
                } }, "BIN", 5 + i);

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    BinaryObject bo = ignite(0).binary().builder("SQL_TYPE")
                        .setField("id", KEY)
                        .setField("str", validStr.toString())
                        .build();

                    tblCache.put(0, bo);

                    return null;
                } }, "STR", 5 + i);

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    BinaryObject bo = ignite(0).binary().builder("SQL_TYPE")
                        .setField("id", KEY)
                        .setField("bin", validStr.toString().getBytes(StandardCharsets.UTF_8))
                        .build();

                    tblCache.put(0, bo);

                    return null;
                } }, "BIN", 5 + i);
        }
    }

    /** */
    @Test
    public void testInsertValueVarColumns() {
        StringBuilder validStr = new StringBuilder("12345");

        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into PERSON(_KEY, name) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into PERSON(_KEY, arr) values (?, ?)");

        for (int i = 1; i < CNT; i++) {
            String v = validStr.append("0").toString();
            int len = 5 + i;

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    cache.put(KEY, new Person(v));

                    return null;
                } }, "NAME", len);

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    cache.put(KEY, new Person(v.getBytes(StandardCharsets.UTF_8)));

                    return null;
                } }, "ARR", len);

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    cache.query(strQry.setArgs(KEY, v));

                    return null;
                } }, "NAME", len);

            assertPrecision(new Callable<Object>() {
                @Override public Object call() {
                    cache.query(binQry.setArgs(KEY, v.getBytes(StandardCharsets.UTF_8)));

                    return null;
                } }, "ARR", len);
        }
    }

    /** */
    @Test
    public void testClientInsertTableVarColumns() throws Exception {
        cache.query(new SqlFieldsQuery("" +
            "create table TABLE(" +
            "   id int PRIMARY KEY," +
            "   str varchar(5)," +
            "   bin binary(5)" +
            ") with \"CACHE_NAME=SQL,VALUE_TYPE=SQL_TYPE\""));

        StringBuilder validStr = new StringBuilder("12345");

        SqlFieldsQuery strQry = new SqlFieldsQuery("insert into TABLE(id, str) values (?, ?)");
        SqlFieldsQuery binQry = new SqlFieldsQuery("insert into TABLE(id, bin) values (?, ?)");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            ClientCache cc = client.cache("SQL");

            for (int i = 1; i < CNT; i++) {
                validStr.append("0");

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        cc.query(strQry.setArgs(KEY, validStr.toString())).getAll();

                        return null;
                    } }, "STR", 5 + i);

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        cc.query(binQry.setArgs(KEY, validStr.toString().getBytes(StandardCharsets.UTF_8))).getAll();

                        return null;
                    } }, "BIN", 5 + i);

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        BinaryObject bo = ignite(0).binary().builder("SQL_TYPE")
                            .setField("id", KEY)
                            .setField("str", validStr.toString())
                            .build();

                        cc.put(0, bo);

                        return null;
                    } }, "STR", 5 + i);

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        BinaryObject bo = ignite(0).binary().builder("SQL_TYPE")
                            .setField("id", KEY)
                            .setField("bin", validStr.toString().getBytes(StandardCharsets.UTF_8))
                            .build();

                        cc.put(0, bo);

                        return null;
                    } }, "BIN", 5 + i);
            }
        }
    }

    /** */
    @Test
    public void testClientInsertValueVarColumns() throws Exception {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
            ClientCache cc = client.cache("PERSON");

            StringBuilder validStr = new StringBuilder("12345");

            SqlFieldsQuery strQry = new SqlFieldsQuery("insert into PERSON(_KEY, name) values (?, ?)");
            SqlFieldsQuery binQry = new SqlFieldsQuery("insert into PERSON(_KEY, arr) values (?, ?)");

            for (int i = 1; i < CNT; i++) {
                String v = validStr.append("0").toString();
                int len = 5 + i;

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        cc.put(KEY, new Person(v));

                        return null;
                    } }, "NAME", len);

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        cc.put(KEY, new Person(v.getBytes(StandardCharsets.UTF_8)));

                        return null;
                    } }, "ARR", len);

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        cc.query(strQry.setArgs(KEY, v)).getAll();

                        return null;
                    } }, "NAME", len);

                assertClientPrecision(new Callable<Object>() {
                    @Override public Object call() {
                        cc.query(binQry.setArgs(KEY, v.getBytes(StandardCharsets.UTF_8))).getAll();

                        return null;
                    } }, "ARR", len);
            }
        }
    }

    /** */
    private void assertPrecision(Callable<Object> clo, String colName, int len) {
        GridTestUtils.assertThrows(null, clo, CacheException.class,
            "Value for a column '" + colName + "' is too long. Maximum length: 5, actual length: " + len);

        assertNull(cache.get(0));
    }

    /** */
    private void assertClientPrecision(Callable<Object> clo, String colName, int len) {
        GridTestUtils.assertThrows(null, clo, ClientException.class,
            "Value for a column '" + colName + "' is too long. Maximum length: 5, actual length: " + len);

        assertNull(cache.get(0));
    }

    /** */
    static class Person {
        /** */
        @QuerySqlField(precision = 5)
        private final String name;

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

        /** {@inheritDoc} */
        @Override public int hashCode() {
            if (name != null)
                return name.hashCode();
            else
                return Arrays.hashCode(arr);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return java.util.Objects.equals(name, person.name) && Arrays.equals(arr, person.arr);
        }
    }
}
