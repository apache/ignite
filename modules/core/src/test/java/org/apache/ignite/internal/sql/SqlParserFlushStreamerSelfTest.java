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
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlFlushStreamerCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;

/**
 * Tests for SQL parser: SET STREAMING.
 */
public class SqlParserFlushStreamerSelfTest extends SqlParserAbstractSelfTest {
    /**
     *
     */
    public void testParseFlushStreamer() {
        parseValidate("flush streamer");

        assertParseError(QueryUtils.DFLT_SCHEMA, "flush",
            "Failed to parse SQL statement \"flush[*]\": Unexpected end of command (expected: \"STREAMER\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "flush something",
            "Failed to parse SQL statement \"set streaming[*]\": Unexpected end of command (expected: " +
                "\"ON\", \"OFF\", \"1\", \"0\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming 500",
            "Failed to parse SQL statement \"set streaming [*]500\": Unexpected token: \"500\" (expected: " +
                "\"ON\", \"OFF\", \"1\", \"0\")");
    }

    /**
     * Parse and validate SQL script.
     *
     * @param sql SQL.
     */
    private static void parseValidate(String sql) {
        SqlCommand cmd = new SqlParser(QueryUtils.DFLT_SCHEMA, sql).nextCommand();

        assertTrue(cmd instanceof SqlFlushStreamerCommand);
    }
}
