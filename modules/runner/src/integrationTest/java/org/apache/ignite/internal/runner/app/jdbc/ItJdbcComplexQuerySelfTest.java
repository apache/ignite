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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for complex queries (joins, etc.).
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcComplexQuerySelfTest extends AbstractJdbcSelfTest {
    @BeforeAll
    public static void createTable() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS public.person");
            s.execute("CREATE TABLE public.person (id INTEGER PRIMARY KEY, orgid INTEGER, "
                    + "name VARCHAR NOT NULL, age INTEGER NOT NULL)");

            s.execute("DROP TABLE IF EXISTS public.org");
            s.execute("CREATE TABLE public.org (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)");

            s.executeUpdate("INSERT INTO public.person(orgid, id, name, age) VALUES "
                    + "(1, 1, 'John White', 25), "
                    + "(1, 2, 'Joe Black', 35), "
                    + "(2, 3, 'Mike Green', 40)");
            s.executeUpdate("INSERT INTO public.org(id, name) VALUES "
                    + "(1, 'A'), "
                    + "(2, 'B')");
        }
    }

    /**
     * Join test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJoin() throws Exception {
        ResultSet rs = stmt.executeQuery(
                "select p.id, p.name, o.name as orgName from PUBLIC.Person p, PUBLIC.Org o where p.orgId = o.id");

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 1) {
                assertEquals("John White", rs.getString("name"));
                assertEquals("A", rs.getString("orgName"));
            } else if (id == 2) {
                assertEquals("Joe Black", rs.getString("name"));
                assertEquals("A", rs.getString("orgName"));
            } else if (id == 3) {
                assertEquals("Mike Green", rs.getString("name"));
                assertEquals("B", rs.getString("orgName"));
            } else {
                fail("Wrong ID: " + id);
            }

            cnt++;
        }

        assertEquals(3, cnt);
    }

    /**
     * Join without alias test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWithoutAlias() throws Exception {
        ResultSet rs = stmt.executeQuery(
                "select p.id, p.name, o.name from PUBLIC.Person p, PUBLIC.Org o where p.orgId = o.id");

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt(1);

            if (id == 1) {
                assertEquals("John White", rs.getString("name"));
                assertEquals("John White", rs.getString(2));
                assertEquals("A", rs.getString(3));
            } else if (id == 2) {
                assertEquals("Joe Black", rs.getString("name"));
                assertEquals("Joe Black", rs.getString(2));
                assertEquals("A", rs.getString(3));
            } else if (id == 3) {
                assertEquals("Mike Green", rs.getString("name"));
                assertEquals("Mike Green", rs.getString(2));
                assertEquals("B", rs.getString(3));
            } else {
                fail("Wrong ID: " + id);
            }

            cnt++;
        }

        assertEquals(3, cnt);
    }

    /**
     * In function test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIn() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from PUBLIC.Person where age in (25, 35)");

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            assertThat(rs.getString("name"), anyOf(is("John White"), is("Joe Black")));

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * Between func test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBetween() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from PUBLIC.Person where age between 24 and 36");

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            assertThat(rs.getString("name"), anyOf(is("John White"), is("Joe Black")));

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * Calculated value test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCalculatedValue() throws Exception {
        ResultSet rs = stmt.executeQuery("select age * 2 from PUBLIC.Person");

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            assertThat(rs.getInt(1), anyOf(is(50), is(70), is(80)));

            cnt++;
        }

        assertEquals(3, cnt);
    }

    /**
     * Wrong argument type test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWrongArgumentType() throws Exception {
        try (ResultSet rs = stmt.executeQuery("select * from PUBLIC.Org where name = '2'")) {
            assertFalse(rs.next());
        }

        // Check non-indexed field.
        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = stmt.executeQuery("select * from PUBLIC.Org where name = 2")) {
                assertFalse(rs.next());
            }
        });

        // Check indexed field.
        try (ResultSet rs = stmt.executeQuery("select * from PUBLIC.Person where name = '2'")) {
            assertFalse(rs.next());
        }

        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = stmt.executeQuery("select * from PUBLIC.Person where name = 2")) {
                assertFalse(rs.next());
            }
        });
    }
}
