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

import org.apache.ignite.configuration.IgniteConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
public class JdbcThinComplexDmlDdlCustomSchemaSelfTest extends JdbcThinComplexDmlDdlSelfTest {
    /** Simple schema. */
    private static final String SCHEMA_1 = "SCHEMA_1";

    /** Complex schema. */
    private static final String SCHEMA_2 = "\"SCHEMA 2\"";

    /** Current schema. */
    private String curSchema = SCHEMA_1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlSchemas(SCHEMA_1, SCHEMA_2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/" + curSchema);
    }

    /**
     * Test create/select/drop flow on escaped schema.
     *
     * @throws Exception If failed.
     */
    public void testCreateSelectDropEscapedSchema() throws Exception {
        try {
            curSchema = SCHEMA_2;

            testCreateSelectDrop();
        }
        finally {
            curSchema = SCHEMA_1;
        }
    }

    /**
     * Test multiple iterations.
     *
     * @throws Exception If failed.
     */
    public void testMultiple() throws Exception {
        testCreateSelectDrop();
        testCreateSelectDrop();
    }
}