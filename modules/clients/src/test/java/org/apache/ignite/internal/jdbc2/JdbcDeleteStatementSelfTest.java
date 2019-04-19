/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.HashSet;
import org.junit.Test;

/**
 *
 */
public class JdbcDeleteStatementSelfTest extends JdbcAbstractUpdateStatementSelfTest {
    /**
     *
     */
    @Test
    public void testExecute() throws SQLException {
        conn.createStatement().execute("delete from Person where cast(substring(_key, 2, 1) as int) % 2 = 0");

        assertFalse(jcache(0).containsKey("p2"));
        assertTrue(jcache(0).containsKeys(new HashSet<Object>(Arrays.asList("p1", "p3"))));
    }

    /**
     *
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        int res =
            conn.createStatement().executeUpdate("delete from Person where cast(substring(_key, 2, 1) as int) % 2 = 0");

        assertEquals(1, res);
        assertFalse(jcache(0).containsKey("p2"));
        assertTrue(jcache(0).containsKeys(new HashSet<Object>(Arrays.asList("p1", "p3"))));
    }

    /**
     *
     */
    @Test
    public void testBatch() throws SQLException {
        PreparedStatement ps = conn.prepareStatement("delete from Person where firstName = ?");

        ps.setString(1, "John");

        ps.addBatch();

        ps.setString(1, "Harry");

        ps.addBatch();

        int[] res = ps.executeBatch();

        assertFalse(jcache(0).containsKey("p1"));
        assertTrue(jcache(0).containsKeys(new HashSet<Object>(Arrays.asList("p2", "p3"))));
        assertTrue(Arrays.equals(new int[] {1, 0}, res));
    }
}
