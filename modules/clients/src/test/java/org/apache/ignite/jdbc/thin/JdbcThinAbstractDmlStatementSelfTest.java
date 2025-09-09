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

package org.apache.ignite.jdbc.thin;

import java.io.Serializable;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;

import static java.nio.charset.StandardCharsets.UTF_16;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public abstract class JdbcThinAbstractDmlStatementSelfTest extends JdbcThinAbstractSelfTest {
    /** SQL SELECT query for verification. */
    static final String SQL_SELECT = "select _key, id, firstName, lastName, age, data, text from Person";

    /** Connection. */
    protected Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite(0).getOrCreateCache(cacheConfig());

        conn = createConnection();

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        conn.close();

        assertTrue(conn.isClosed());
    }

    /**
     * @return JDBC connection.
     * @throws SQLException On error.
     */
    protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration0(igniteInstanceName);
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Grid configuration used for starting the grid.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration0(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Grid configuration used for starting the grid ready for manipulating binary objects.
     * @throws Exception If failed.
     */
    IgniteConfiguration getBinaryConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = getConfiguration0(igniteInstanceName);

        CacheConfiguration ccfg = cfg.getCacheConfiguration()[0];

        ccfg.getQueryEntities().clear();

        QueryEntity e = new QueryEntity();

        e.setKeyType(String.class.getName());
        e.setValueType("Person");

        e.addQueryField("id", Integer.class.getName(), null);
        e.addQueryField("age", Integer.class.getName(), null);
        e.addQueryField("firstName", String.class.getName(), null);
        e.addQueryField("lastName", String.class.getName(), null);
        e.addQueryField("data", byte[].class.getName(), null);
        e.addQueryField("text", String.class.getName(), null);

        ccfg.setQueryEntities(Collections.singletonList(e));

        return cfg;
    }

    /**
     * @return Cache configuration for non binary marshaller tests.
     */
    private CacheConfiguration nonBinCacheConfig() {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Person.class
        );

        return cache;
    }

    /**
     * @return Cache configuration for binary marshaller tests.
     */
    final CacheConfiguration binaryCacheConfig() {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity e = new QueryEntity();

        e.setKeyType(String.class.getName());
        e.setValueType("Person");

        e.addQueryField("id", Integer.class.getName(), null);
        e.addQueryField("age", Integer.class.getName(), null);
        e.addQueryField("firstName", String.class.getName(), null);
        e.addQueryField("lastName", String.class.getName(), null);
        e.addQueryField("data", byte[].class.getName(), null);
        e.addQueryField("text", String.class.getName(), null);

        cache.setQueryEntities(Collections.singletonList(e));

        return cache;
    }

    /**
     * @return Configuration of cache to create.
     */
    CacheConfiguration cacheConfig() {
        return nonBinCacheConfig();
    }

    /**
     * Helper to get test binary data as string UTF-16 encoding to be in sync with the RAWTOHEX function
     * which uses UTF-16 for conversion strings to byte arrays.
     * @param str String.
     * @return Byte array with the UTF-16 encoding.
     */
    static byte[] getBytes(String str) {
        return str.getBytes(UTF_16);
    }

    /**
     * Helper to convert a binary data (which is a string UTF-16 encoding) back to string.
     * @param arr Byte array with the UTF-16 encoding.
     * @return String.
     */
    static String str(byte[] arr) {
        return new String(arr, UTF_16);
    }

    /**
     * @param blob Blob.
     */
    static byte[] getBytes(Blob blob) {
        try {
            return blob.getBytes(1, (int)blob.length());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param clob Clob.
     */
    static String str(Clob clob) {
        try {
            return clob.getSubString(1, (int)clob.length());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Person.
     */
    static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField
        private final String firstName;

        /** Last name. */
        @QuerySqlField
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /** Binary data (BLOB). */
        @QuerySqlField
        private final byte[] data;

        /** CLOB. */
        @QuerySqlField
        private final String text;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.data = getBytes(lastName);
            this.text = firstName + " " + lastName;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person)o;

            if (id != person.id) return false;
            if (age != person.age) return false;
            if (firstName != null ? !firstName.equals(person.firstName) : person.firstName != null) return false;
            if (lastName != null ? !lastName.equals(person.lastName) : person.lastName != null) return false;
            if (data != null ? !Arrays.equals(data, person.data) : person.data != null) return false;
            if (text != null ? !text.equals(person.text) : person.text != null) return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;

            result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            result = 31 * result + age;
            result = 31 * result + (data != null ? Arrays.hashCode(data) : 0);
            result = 31 * result + (text != null ? text.hashCode() : 0);

            return result;
        }
    }
}
