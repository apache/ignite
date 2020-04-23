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
import org.junit.Test;

/**
 * Tests for SQL parser: SET STREAMING.
 */
public class SqlParserSetStreamingSelfTest extends SqlParserAbstractSelfTest {
    /**
     *
     */
    @Test
    public void testParseSetStreaming() {
        parseValidate("set streaming on", true, false, 2048, 0, 0, 0, false);
        parseValidate("set streaming 1", true, false, 2048, 0, 0, 0, false);
        parseValidate("set streaming off", false, false, 2048, 0, 0, 0, false);
        parseValidate("set streaming 0", false, false, 2048, 0, 0, 0, false);
        parseValidate("set streaming on batch_size 100", true, false, 100, 0, 0, 0, false);
        parseValidate("set streaming on flush_frequency 500", true, false, 2048, 0, 0, 500, false);
        parseValidate("set streaming on per_node_buffer_size 100", true, false, 2048, 0, 100, 0, false);
        parseValidate("set streaming on per_node_parallel_operations 4", true, false, 2048, 4, 0, 0, false);
        parseValidate("set streaming on allow_overwrite on", true, true, 2048, 0, 0, 0, false);
        parseValidate("set streaming on allow_overwrite off", true, false, 2048, 0, 0, 0, false);
        parseValidate("set streaming on per_node_buffer_size 50 flush_frequency 500 " +
            "per_node_parallel_operations 4 allow_overwrite on batch_size 100", true, true, 100, 4, 50, 500, false);

        parseValidate("set streaming on ordered", true, false, 2048, 0, 0, 0, true);
        parseValidate("set streaming 1 ordered", true, false, 2048, 0, 0, 0, true);
        parseValidate("set streaming on batch_size 100 ordered", true, false, 100, 0, 0, 0, true);
        parseValidate("set streaming on per_node_buffer_size 50 flush_frequency 500 " +
            "per_node_parallel_operations 4 allow_overwrite on batch_size 100 ordered", true, true, 100, 4, 50, 500, true);

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

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming on allow_overwrite",
            "set streaming on allow_overwrite[*]\": Unexpected end of command (expected: \"ON\", \"OFF\", \"1\", " +
                "\"0\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming 1 batch_size",
            "Failed to parse SQL statement \"set streaming 1 batch_size[*]\": Unexpected end of command " +
                "(expected: \"[integer]\")");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming on per_node_parallel_operations -4",
            "Failed to parse SQL statement \"set streaming on per_node_parallel_operations -[*]4\": " +
                "Invalid per node parallel operations number (must be positive)");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming on per_node_buffer_size -4",
            "Failed to parse SQL statement \"set streaming on per_node_buffer_size -[*]4\": " +
                "Invalid per node buffer size (must be positive)");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming on flush_frequency -4",
            "Failed to parse SQL statement \"set streaming on flush_frequency -[*]4\": " +
                "Invalid flush frequency (must be positive)");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off allow_overwrite",
            "Failed to parse SQL statement \"set streaming off [*]allow_overwrite\": Unexpected token: " +
                "\"ALLOW_OVERWRITE\"");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off ordered",
            "Failed to parse SQL statement \"set streaming off [*]ordered\": Unexpected token: " +
                "\"ORDERED\"");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off batch_size",
            "Failed to parse SQL statement \"set streaming off [*]batch_size\": Unexpected token: " +
                "\"BATCH_SIZE\"");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off flush_frequency",
            "Failed to parse SQL statement \"set streaming off [*]flush_frequency\": Unexpected token: " +
                "\"FLUSH_FREQUENCY\"");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off per_node_buffer_size",
            "Failed to parse SQL statement \"set streaming off [*]per_node_buffer_size\": Unexpected token: " +
                "\"PER_NODE_BUFFER_SIZE\"");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off per_node_parallel_operations",
            "Failed to parse SQL statement \"set streaming off [*]per_node_parallel_operations\": Unexpected token: " +
                "\"PER_NODE_PARALLEL_OPERATIONS\"");

        assertParseError(QueryUtils.DFLT_SCHEMA, "set streaming off table",
            "Failed to parse SQL statement \"set streaming off [*]table\": Unexpected token: \"TABLE\"");

    }

    /**
     * Parse and validate SQL script.
     *
     * @param sql SQL.
     * @param expOn Expected on/off value.
     * @param expAllowOverwrite Expected allow overwrite flag.
     * @param expBatchSize Expected batch size.
     * @param expParOps Expected per-node parallael operations.
     * @param expBufSize Expected per node buffer size.
     * @param expFlushFreq Expected flush frequency.
     * @param ordered Ordered stream flag.
     */
    private static void parseValidate(String sql, boolean expOn, boolean expAllowOverwrite, int expBatchSize,
        int expParOps, int expBufSize, long expFlushFreq, boolean ordered) {
        SqlSetStreamingCommand cmd = (SqlSetStreamingCommand)new SqlParser(QueryUtils.DFLT_SCHEMA, sql).nextCommand();

        assertEquals(expOn, cmd.isTurnOn());

        assertEquals(expAllowOverwrite, cmd.allowOverwrite());

        assertEquals(expBatchSize, cmd.batchSize());

        assertEquals(expParOps, cmd.perNodeParallelOperations());

        assertEquals(expBufSize, cmd.perNodeBufferSize());

        assertEquals(expFlushFreq, cmd.flushFrequency());

        assertEquals(ordered, cmd.isOrdered());
    }
}
