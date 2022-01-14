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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Delete functional statement self test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcDeleteStatementSelfTest extends ItJdbcAbstractStatementSelfTest {
    /**
     * Execute delete query test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        stmt.execute("delete from PUBLIC.PERSON where substring(SID, 2, 1)::int % 2 = 0");

        KeyValueView<Tuple, Tuple> kvView = clusterNodes.get(0).tables().table("PUBLIC.PERSON").keyValueView();

        assertFalse(kvView.contains(null, Tuple.create().set("ID", 2)));
        assertTrue(kvView.contains(null, Tuple.create().set("ID", 1)));
        assertTrue(kvView.contains(null, Tuple.create().set("ID", 3)));
    }

    /**
     * Execute delete update test.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        int res = stmt.executeUpdate("delete from PUBLIC.PERSON where substring(SID, 2, 1)::int % 2 = 0");

        assertEquals(1, res);

        KeyValueView<Tuple, Tuple> kvView = clusterNodes.get(0).tables().table("PUBLIC.PERSON").keyValueView();

        assertFalse(kvView.contains(null, Tuple.create().set("ID", 2)));
        assertTrue(kvView.contains(null, Tuple.create().set("ID", 1)));
        assertTrue(kvView.contains(null, Tuple.create().set("ID", 3)));
    }
}
