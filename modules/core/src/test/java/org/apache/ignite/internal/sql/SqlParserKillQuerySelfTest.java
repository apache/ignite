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

/**
 * Tests for processing of KILL QUERY syntax.
 */
public class SqlParserKillQuerySelfTest extends SqlParserAbstractSelfTest {
    /** */
    private static final String TEST_UUID = "b3c0624a-122c-46ea-9d65-67b56df00001";
    /** */
    private static final long TEST_QRY_ID = 341;

    /**
     * Tests for KILL QUERY command.
     */
    public void testKillQuery() {
        UUID uuid = UUID.randomUUID();

        long qryId = ThreadLocalRandom.current().nextLong();

        assertKillQuery("KILL QUERY " + uuid.toString() + " " + qryId, uuid, qryId);

        assertKillQuery("KILL QUERY " + TEST_UUID + " " + TEST_QRY_ID, UUID.fromString(TEST_UUID), TEST_QRY_ID);

        assertKillQuery("kill query " + TEST_UUID + " " + TEST_QRY_ID, UUID.fromString(TEST_UUID), TEST_QRY_ID);

        assertKillQuery("kIlL qUeRy " + TEST_UUID + " " + TEST_QRY_ID, UUID.fromString(TEST_UUID), TEST_QRY_ID);

        assertKillQuery("KILL QUERY " + TEST_UUID.toUpperCase() + " " + TEST_QRY_ID, UUID.fromString(TEST_UUID), TEST_QRY_ID);

        assertParseError("KILL " + TEST_UUID + " " + TEST_QRY_ID, "Unexpected token: \"B3C0624A\" (expected: \"QUERY\")");

        assertParseError("KILL QUERY", "Unexpected end of command (expected: \"[UUID]\")");

        assertParseError("KILL QUERY b3c0624a122c-46ea-9d65-67b56df00001 123", "Unexpected token: \"67B56DF00001\" (expected: \"[UUID]");

        assertParseError("KILL QUERY b3c0624a-122c1-46ea-9d65-67b56df00001 123", "Unexpected token: \"67B56DF00001\" (expected: \"[UUID]\"");

        assertParseError("KILL QUERY " + TEST_UUID, "Unexpected end of command (expected: \"[long]\")");

        assertParseError("KILL QUERY " + TEST_UUID + " ", "Unexpected end of command (expected: \"[long]\")");

        assertParseError("KILL QUERY " + TEST_UUID + " aaa", "Unexpected token: \"AAA\" (expected: \"[long]\")");

        assertParseError("KILL QUERY " + TEST_UUID + " " + TEST_QRY_ID + " " + 1, "Unexpected token: \"1\"");

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
     * @param uuidExp Expected UUID.
     * @param qryIdExp Expected query id.
     */
    private static void assertKillQuery(String sql, UUID uuidExp, long qryIdExp) {
        SqlCommand cmd = parse(sql);

        Assert.assertTrue(cmd instanceof SqlKillQueryCommand);

        SqlKillQueryCommand killQryCmd = (SqlKillQueryCommand)cmd;

        Assert.assertEquals(uuidExp, killQryCmd.getNodeId());

        Assert.assertEquals(qryIdExp, killQryCmd.getNodeQryId());
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
