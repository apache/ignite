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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.internal.jdbc2.JdbcAbstractSchemaCaseTest;

/**
 * Jdbc thin test for schema name case (in)sensitivity.
 */
public class JdbcThinSchemaCaseSelfTest extends JdbcAbstractSchemaCaseTest {
    /** {@inheritDoc} */
    @Override protected Connection connect(String schema) throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/" + schema);
    }
}
