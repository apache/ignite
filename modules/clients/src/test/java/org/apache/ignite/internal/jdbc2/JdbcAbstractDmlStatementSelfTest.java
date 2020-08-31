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

package org.apache.ignite.internal.jdbc2;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public abstract class JdbcAbstractDmlStatementSelfTest extends GridCommonAbstractTest {
    /** UTF 16 character set name. */
    private static final String UTF_16 = "UTF-16"; // RAWTOHEX function use UTF-16 for conversion strings to byte arrays.

    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "cache=" + DEFAULT_CACHE_NAME + "@modules/clients/src/test/config/jdbc-config.xml";

    /** JDBC URL for tests involving binary objects manipulation. */
    static final String BASE_URL_BIN = CFG_URL_PREFIX + "cache=" + DEFAULT_CACHE_NAME + "@modules/clients/src/test/config/jdbc-bin-config.xml";

    /** SQL SELECT query for verification. */
    static final String SQL_SELECT = "select _key, id, firstName, lastName, age, data from Person";

    /** Alias for _key */
    private static final String KEY_ALIAS = "key";

    /** Connection. */
    protected Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ((IgniteEx)ignite(0)).context().cache().dynamicStartSqlCache(cacheConfig());

        conn = DriverManager.getConnection(getCfgUrl());
    }

    /**
     * @return Cache configuration for non binary marshaller tests.
     */
    private CacheConfiguration nonBinCacheConfig() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

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
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity e = new QueryEntity();

        e.setKeyType(String.class.getName());
        e.setValueType("Person");

        e.setKeyFieldName(KEY_ALIAS);

        e.addQueryField(KEY_ALIAS, e.getKeyType(), null);
        e.addQueryField("id", Integer.class.getName(), null);
        e.addQueryField("age", Integer.class.getName(), null);
        e.addQueryField("firstName", String.class.getName(), null);
        e.addQueryField("lastName", String.class.getName(), null);
        e.addQueryField("data", byte[].class.getName(), null);

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
     * @return URL of XML configuration file.
     */
    protected String getCfgUrl() {
        return BASE_URL;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ((IgniteEx)ignite(0)).context().cache().dynamicDestroyCache(DEFAULT_CACHE_NAME, true, true, false, null);

        if (conn != null) {
            conn.close();
            assertTrue(conn.isClosed());
        }

        cleanUpWorkingDir();
    }

    /**
     * Clean up working directory.
     */
    private void cleanUpWorkingDir() throws Exception {
        String workDir = U.defaultWorkDirectory();

        U.delete(U.resolveWorkDirectory(workDir, DataStorageConfiguration.DFLT_MARSHALLER_PATH, false));
    }

    /**
     * @param str String.
     */
    static byte[] getBytes(String str) {
        try {
            return str.getBytes(UTF_16);
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
     * @param arr Array.
     */
    static String str(byte[] arr) {
        try {
            return new String(arr, UTF_16);
        }
        catch (UnsupportedEncodingException e) {
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

        /** Binary data. */
        @QuerySqlField
        private final byte[] data;

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
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (id != person.id) return false;
            if (age != person.age) return false;
            if (firstName != null ? !firstName.equals(person.firstName) : person.firstName != null) return false;
            return lastName != null ? lastName.equals(person.lastName) : person.lastName == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            result = 31 * result + age;
            return result;
        }
    }
}
