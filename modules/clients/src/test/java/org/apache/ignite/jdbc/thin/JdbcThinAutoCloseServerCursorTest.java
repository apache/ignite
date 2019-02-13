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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests an optional optimization that server cursor is closed automatically
 * when last result set page is transmitted.
 */
public class JdbcThinAutoCloseServerCursorTest extends JdbcThinAbstractSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/?autoCloseServerCursor=true";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(CACHE_NAME);
        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(CACHE_NAME);

        cache.clear();
    }

    /**
     * Ensure that server cursor is implicitly closed on last page.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQuery() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(CACHE_NAME);

        Person persons[] = new Person[] {
            new Person(1, "John", 25),
            new Person(2, "Mary", 23)
        };

        for (Person person: persons)
            cache.put(person.id, person);

        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema('"' + CACHE_NAME + '"');

            String sqlText = "select * from Person";

            try (Statement stmt = conn.createStatement()) {
                // Ensure that result set fits into one page
                stmt.setFetchSize(2);

                try (ResultSet rs = stmt.executeQuery(sqlText)) {
                    // Attempt to get query metadata when server cursor is already closed
                    GridTestUtils.assertThrows(log(), new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                return rs.getMetaData();
                            }
                        },
                        SQLException.class,
                        "Server cursor is already closed"
                    );

                    GridTestUtils.assertThrows(log(), new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                return rs.findColumn("id");
                            }
                        },
                        SQLException.class,
                        "Server cursor is already closed"
                    );

                    checkResultSet(rs, persons);
                }
            }

            try (Statement stmt = conn.createStatement()) {
                // Ensure multiple page result set
                stmt.setFetchSize(1);

                try (ResultSet rs = stmt.executeQuery(sqlText)) {
                    // Getting result set metadata is OK here
                    assertEquals(3, rs.getMetaData().getColumnCount());

                    assertEquals(1, rs.findColumn("id"));

                    checkResultSet(rs, persons);
                }
            }

            try (Statement stmt = conn.createStatement()) {
                // Ensure multiple page result set
                stmt.setFetchSize(1);

                try (ResultSet rs = stmt.executeQuery(sqlText)) {
                    checkResultSet(rs, persons);

                    // Server cursor is closed now
                    GridTestUtils.assertThrows(log(), new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                return rs.getMetaData();
                            }
                        },
                        SQLException.class,
                        "Server cursor is already closed"
                    );

                    GridTestUtils.assertThrows(log(), new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                return rs.findColumn("id");
                            }
                        },
                        SQLException.class,
                        "Server cursor is already closed"
                    );
                }
            }
        }
    }

    /**
     * Ensure that insert works when auto close of server cursor is enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInsert() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema('"' + CACHE_NAME + '"');

            String sqlText = "insert into Person (_key, id, name, age) values (?, ?, ?, ?)";

            Person p = new Person(1, "John", 25);

            try (PreparedStatement prepared = conn.prepareStatement(sqlText)) {
                prepared.setInt(1, p.id);
                prepared.setInt(2, p.id);
                prepared.setString(3, p.name);
                prepared.setInt(4, p.age);

                assertFalse(prepared.execute());
                assertEquals(1, prepared.getUpdateCount());
            }

            IgniteCache<Integer, Person> cache = grid(0).cache(CACHE_NAME);

            assertEquals(p, cache.get(1));
        }
    }

    /**
     * Ensure that update works when auto close of server cursor is enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(CACHE_NAME);

        Person p = new Person(1, "John", 25);

        cache.put(1, p);

        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema('"' + CACHE_NAME + '"');

            String sqlText = "update Person set age = age + 1";

            try (Statement stmt = conn.createStatement()) {
                assertEquals(1, stmt.executeUpdate(sqlText));
            }

            assertEquals(p.age + 1, cache.get(1).age);
        }
    }

    /**
     * Ensure that delete works when auto close of server cursor is enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDelete() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(CACHE_NAME);

        Person p = new Person(1, "John", 25);

        cache.put(1, p);

        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema('"' + CACHE_NAME + '"');

            String sqlText = "delete Person where age = ?";

            try (PreparedStatement prepared = conn.prepareStatement(sqlText)) {
                prepared.setInt(1, p.age);

                assertEquals(1, prepared.executeUpdate());
            }

            assertNull(cache.get(1));
        }
    }

    /**
     * Checks result set against array of Person.
     *
     * @param rs Result set.
     * @param persons Array of Person.
     * @throws Exception If failed.
     */
    private void checkResultSet(ResultSet rs, Person[] persons) throws Exception {
        while (rs.next()) {
            Person p = new Person(
                rs.getInt(1),
                rs.getString(2),
                rs.getInt(3));

            assert p.id > 0 && p.id <= persons.length;

            assertEquals(persons[p.id - 1], p);
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
        private final String name;

        /** Last name. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param name Name.
         * @param age Age.
         */
        Person(int id, String name, int age) {
            assert !F.isEmpty(name);
            assert age > 0;

            this.id = id;
            this.name = name;
            this.age = age;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person) o;

            if (id != person.id)
                return false;

            if (name == null ^ person.name == null)
                return false;

            if (name != null && !name.equals(person.name))
                return false;

            return age == person.age;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;

            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + age;

            return result;
        }
    }
}
