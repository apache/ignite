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

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.command.SqlRefreshStatitsicsCommand;
import org.junit.Test;

/**
 * Test for SQL parser: REFRESH STATISTICS command.
 */
public class SqlParserRefreshStatisticsSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for REFRESH STATISTICS command.
     */
    @Test
    public void testRefresh() {
        parseValidate(null, "REFRESH STATISTICS tbl", new StatisticsTarget((String)null, "TBL"));
        parseValidate(null, "REFRESH STATISTICS tbl;", new StatisticsTarget((String)null, "TBL"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl",
            new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl;",
            new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl(a)",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl(a);",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS tbl(a)",
            new StatisticsTarget((String)null, "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS tbl(a);",
            new StatisticsTarget((String)null, "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS tbl(a, b, c)",
            new StatisticsTarget((String)null, "TBL", "A", "B", "C"));
        parseValidate(null, "REFRESH STATISTICS tbl(a), schema.tbl2(a,B)",
            new StatisticsTarget((String)null, "TBL", "A"),
            new StatisticsTarget("SCHEMA", "TBL2", "A", "B"));

        assertParseError(null, "REFRESH STATISTICS p,", "Unexpected end of command");
        assertParseError(null, "REFRESH STATISTICS p()", "Unexpected token: \")\"");
    }

    /**
     * Parse command and validate it.
     *
     * @param schema Schema.
     * @param sql SQL text.
     * @param targets Expected targets.
     */
    private void parseValidate(String schema, String sql, StatisticsTarget... targets) {
        SqlRefreshStatitsicsCommand cmd = (SqlRefreshStatitsicsCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, targets);
    }

    /**
     * Validate command.
     *
     * @param cmd Command to validate.
     * @param targets Expected targets.
     */
    private static void validate(SqlRefreshStatitsicsCommand cmd, StatisticsTarget... targets) {
        assertEquals(cmd.targets().size(), targets.length);

        for (StatisticsTarget target : targets)
            assertTrue(cmd.targets().contains(target));
    }
}
