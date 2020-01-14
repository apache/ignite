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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 *
 */
public class JdbcUpdateStatementSelfTest extends JdbcAbstractUpdateStatementSelfTest {
    /**
     *
     */
    @Test
    public void testExecute() throws SQLException {
        conn.createStatement().execute("update Person set firstName = 'Jack' where " +
            "cast(substring(_key, 2, 1) as int) % 2 = 0");

        assertEquals(Arrays.asList(F.asList("John"), F.asList("Jack"), F.asList("Mike")),
            jcache(0).query(new SqlFieldsQuery("select firstName from Person order by _key")).getAll());
    }

    /**
     *
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        conn.createStatement().executeUpdate("update Person set firstName = 'Jack' where " +
                "cast(substring(_key, 2, 1) as int) % 2 = 0");

        assertEquals(Arrays.asList(F.asList("John"), F.asList("Jack"), F.asList("Mike")),
                jcache(0).query(new SqlFieldsQuery("select firstName from Person order by _key")).getAll());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch() throws SQLException {
        PreparedStatement ps = conn.prepareStatement("update Person set lastName = concat(firstName, 'son') " +
            "where firstName = ?");

        ps.setString(1, "John");

        ps.addBatch();

        ps.setString(1, "Harry");

        ps.addBatch();

        int[] res = ps.executeBatch();

        assertEquals(Arrays.asList(F.asList("Johnson"), F.asList("Black"), F.asList("Green")),
            jcache(0).query(new SqlFieldsQuery("select lastName from Person order by _key")).getAll());

        assertTrue(Arrays.equals(new int[] {1, 0}, res));
    }
}
