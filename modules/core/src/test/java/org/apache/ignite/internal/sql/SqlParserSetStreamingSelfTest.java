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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;

/**
 * Tests for SQL parser: SET STREAMING.
 */
public class SqlParserSetStreamingSelfTest extends SqlParserAbstractSelfTest {
    /**
     *
     */
    public void testParseSetStreaming() {
        parseValidate("set streaming on", true);
        parseValidate("set streaming 1", true);
        parseValidate("set streaming off", false);
        parseValidate("set streaming 0", false);
        parseValidate("set streaming on;", true);

        assertParseError(QueryUtils.DFLT_SCHEMA, "set",
            "Failed to parse SQL statement \"set[*]\": Unexpected end of command (expected: \"STREAMING\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming",
            "Failed to parse SQL statement \"set streaming[*]\": Unexpected end of command (expected: " +
                "\"ON\", \"OFF\", \"1\", \"0\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming table",
            "Failed to parse SQL statement \"set streaming [*]table\": Unexpected token: \"TABLE\" (expected: " +
                "\"ON\", \"OFF\", \"1\", \"0\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming -1",
            "Failed to parse SQL statement \"set streaming [*]-1\": Unexpected token: \"-\" (expected: " +
                "\"ON\", \"OFF\", \"1\", \"0\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming 500",
            "Failed to parse SQL statement \"set streaming [*]500\": Unexpected token: \"500\" (expected: " +
                "\"ON\", \"OFF\", \"1\", \"0\")");
    }

    /**
     * Parse and validate SQL script.
     *
     * @param sql SQL.
     * @param expOn Expected return value of {@link  SqlSetStreamingCommand#turnOn}.
     */
    private static void parseValidate(String sql, boolean expOn) {
        SqlSetStreamingCommand cmd = (SqlSetStreamingCommand)new SqlParser(QueryUtils.DFLT_SCHEMA, sql).nextCommand();

        assertEquals(expOn, cmd.isTurnOn());
    }
}
