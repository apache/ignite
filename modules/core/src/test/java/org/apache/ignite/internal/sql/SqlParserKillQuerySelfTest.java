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
 *
 */

package org.apache.ignite.internal.sql;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlKillQueryCommand;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for processing of KILL QUERY syntax.
 */
@RunWith(JUnit4.class)
public class SqlParserKillQuerySelfTest extends SqlParserAbstractSelfTest {
    /** */
    private static final long TEST_QRY_ID = 341;

    /**
     * Tests for KILL QUERY command.
     */
    @Test
    public void testKillQuery() {
        UUID nodeId = UUID.randomUUID();

        long qryId = ThreadLocalRandom.current().nextLong();

        assertKillQuery("KILL QUERY '" + nodeId + "_" + qryId + "'", nodeId, qryId, false);

        assertKillQuery("KILL QUERY '" + nodeId + "_" + TEST_QRY_ID + "'", nodeId, TEST_QRY_ID, false);

        assertKillQuery("kill query '" + nodeId + "_" + TEST_QRY_ID + "'", nodeId, TEST_QRY_ID, false);

        assertKillQuery("kIlL qUeRy '" + nodeId + "_" + TEST_QRY_ID + "'", nodeId, TEST_QRY_ID, false);

        assertKillQuery("KILL QUERY '" + nodeId + "_" + TEST_QRY_ID + "'", nodeId, TEST_QRY_ID, false);

        assertParseError("KILL QUERY '" + nodeId + "_*1'", expectedExceptionMessageIncorrectQueryId());

        assertParseError("KILL QUERY '" + nodeId + "_a'", expectedExceptionMessageIncorrectQueryId());

        assertParseError("KILL QUERY '" + nodeId + "_1 1'", expectedExceptionMessageIncorrectQueryId());

        assertParseError("KILL QUERY 1'", expectedExceptionMessageNoAsyncAndQueryId());

        assertParseError("KILL QUERY '" + nodeId + "_1' 1", "Unexpected token: \"1\"");

        assertParseError("KILL '" + nodeId + "_" + TEST_QRY_ID + "'", "Unexpected token: \"" + nodeId + "_341\" (expected: \"QUERY\")");

        assertParseError("KILL QUERY ", expectedExceptionMessageIncorrectQueryId());

        assertParseError("KILL QUERY", expectedExceptionMessageIncorrectQueryId());

        assertParseError("KILL QUERY " + nodeId + "_123", expectedExceptionMessageIncorrectQueryId());

        assertKillQuery("KILL QUERY ASYNC '" + nodeId + "_" + qryId + "'", nodeId, qryId, true);

        assertParseError("KILL QUERY ASYNC 1 '" + nodeId + "_" + qryId + "'", expectedExceptionMessageNoQueryId());

        assertParseError("KILL QUERY ASYNC ", expectedExceptionMessageNoQueryId());

    }

    /**
     * @return Expected exception message.
     */
    private String expectedExceptionMessageIncorrectQueryId() {
        return "Global query id should have format '{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";
    }

    /**
     * @return Expected exception message.
     */
    private String expectedExceptionMessageNoAsyncAndQueryId() {
        return "Expected ASYNC token or global query id. Global query id should have format '{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";
    }

    /**
     * @return Expected exception message.
     */
    private String expectedExceptionMessageNoQueryId() {
        return "Expected global query id. Global query id should have format '{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";
    }

    /**
     * Make sure that parse error occurs.
     *
     * @param sql SQL.
     * @param msg Expected error message.
     */
    private static void assertParseError(final String sql, String msg) {
        assertParseError(null, sql, msg);
    }

    /**
     * Test that given SQL is parsed as a KILL QUERY command and all parsed parameters have expected values.
     *
     * @param sql command.
     * @param nodeIdExp Expected UUID.
     * @param qryIdExp Expected query id.
     */
    private static void assertKillQuery(String sql, UUID nodeIdExp, long qryIdExp, boolean async) {
        SqlCommand cmd = parse(sql);

        Assert.assertTrue(cmd instanceof SqlKillQueryCommand);

        SqlKillQueryCommand killQryCmd = (SqlKillQueryCommand)cmd;

        Assert.assertEquals(nodeIdExp, killQryCmd.nodeId());

        Assert.assertEquals(qryIdExp, killQryCmd.nodeQueryId());

        Assert.assertEquals(async, killQryCmd.async());
    }

    /**
     * Parse single SQL command.
     *
     * @param sql command.
     * @return parsed command.
     */
    private static SqlCommand parse(String sql) {
        return new SqlParser(null, sql).nextCommand();
    }
}
