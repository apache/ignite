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

import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlKillLockCommand;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for parsing of KILL LOCK syntax.
 */
@RunWith(JUnit4.class)
public class SqlParserKillLockSelfTest extends SqlParserAbstractSelfTest {
    /** */
    @Test
    public void testKillLock() {
        IgniteUuid xid = IgniteUuid.randomUuid();

        SqlCommand cmd = new SqlParser(null, "KILL LOCK '" + xid + "'").nextCommand();

        assertTrue(cmd instanceof SqlKillLockCommand);

        assertEquals(xid, ((SqlKillLockCommand)cmd).getXid());

        assertParseError(null, "KILL LOCK '123'", "Failed to parse lock ID");
        assertParseError(null, "KILL LOCK 123", "Expected lock ID");
        assertParseError(null, "KILL LOCK", "Expected lock ID");
    }
}
