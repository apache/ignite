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

import java.sql.Statement;

/**
 *
 */
public abstract class JdbcAbstractUpdateStatementSelfTest extends JdbcAbstractDmlStatementSelfTest {
    /** SQL query to populate cache. */
    private static final String ITEMS_SQL = "insert into Person(_key, id, firstName, lastName, age, data) values " +
        "('p1', 1, 'John', 'White', 25, RAWTOHEX('White')), " +
        "('p2', 2, 'Joe', 'Black', 35, RAWTOHEX('Black')), " +
        "('p3', 3, 'Mike', 'Green', 40, RAWTOHEX('Green'))";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        jcache(0).clear();
        try (Statement s = conn.createStatement()) {
            s.executeUpdate(ITEMS_SQL);
        }
    }
}
