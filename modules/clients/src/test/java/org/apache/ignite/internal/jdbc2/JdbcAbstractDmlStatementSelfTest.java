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
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public abstract class JdbcAbstractDmlStatementSelfTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "modules/clients/src/test/config/jdbc-config.xml";

    /** JDBC URL for tests involving binary objects manipulation. */
    static final String BASE_URL_BIN = CFG_URL_PREFIX + "modules/clients/src/test/config/jdbc-bin-config.xml";

    /** SQL SELECT query for verification. */
    static final String SQL_SELECT = "select _key, id, firstName, lastName, age from Person";

    /** Connection. */
    protected Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(getCfgUrl());

        ignite(0).getOrCreateCache(cacheConfig());
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

        e.addQueryField("id", Integer.class.getName(), null);
        e.addQueryField("age", Integer.class.getName(), null);
        e.addQueryField("firstName", String.class.getName(), null);
        e.addQueryField("lastName", String.class.getName(), null);

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
        grid(0).destroyCache(null);

        conn.close();
        assertTrue(conn.isClosed());
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
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
