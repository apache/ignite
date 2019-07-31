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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.jdbc.thin.JdbcThinQueryMemoryTrackerSelfTest;

/**
 * Query memory manager for local queries.
 */
public class JdbcQueryMemoryTrackerSelfTest extends JdbcThinQueryMemoryTrackerSelfTest {

    /** {@inheritDoc} */
    @Override protected Connection createConnection(boolean lazy) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://" +
            "queryMaxMemory=" + (maxMem) + ":lazy=" + lazy +
            "@modules/clients/src/test/config/jdbc-config.xml"
        );

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }
}