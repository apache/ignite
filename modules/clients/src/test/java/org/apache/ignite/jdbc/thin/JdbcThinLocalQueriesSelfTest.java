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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test that replicated-only query is executed locally.
 */
public class JdbcThinLocalQueriesSelfTest extends JdbcThinAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void testLocalThinJdbcQuery() throws SQLException {
        try (Connection c = connect(grid(0), "replicatedOnly=true")) {
            execute(c, "CREATE TABLE Company(id int primary key, name varchar) WITH " +
                "\"template=replicated,cache_name=Company\"");

            execute(c, "CREATE TABLE Person(id int primary key, name varchar, companyid int) WITH " +
                "\"template=replicated,cache_name=Person\"");

            execute(c, "insert into Company(id, name) values (1, 'Apple')");

            execute(c, "insert into Person(id, name, companyid) values (2, 'John', 1)");

            List<List<?>> res = execute(c, "SELECT p.id, p.name, c.name from Person p left join Company c on " +
                "p.companyid = c.id");

            assertEqualsCollections(F.asList(2, "John", "Apple"), res.get(0));
        }
    }
}
