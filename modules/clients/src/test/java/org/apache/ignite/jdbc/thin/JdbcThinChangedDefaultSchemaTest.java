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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.IgniteSystemProperties;

/**
 *
 */
public class JdbcThinChangedDefaultSchemaTest extends JdbcThinNoDefaultSchemaTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_SCHEMA, "\"cache2\"");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_SCHEMA);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            stmt.execute("select t._key, t._val from Integer t");

            conn.setSchema("\"cache1\"");

            stmt = conn.createStatement();

            //Must not affects previous created statements.
            conn.setSchema("invalid_schema");

            stmt.execute("select t._key, t._val from Integer t");

            ResultSet rs = stmt.getResultSet();

            while (rs.next())
                assertEquals(rs.getInt(2), rs.getInt(1) * 2);
        }
    }
}
