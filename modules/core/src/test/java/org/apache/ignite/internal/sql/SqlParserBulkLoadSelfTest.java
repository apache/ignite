/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.sql;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for SQL parser: COPY command.
 */
@RunWith(JUnit4.class)
public class SqlParserBulkLoadSelfTest extends SqlParserAbstractSelfTest {
    /** Tests for COPY command. */
    @Test
    public void testCopy() {
        assertParseError(null,
            "copy grom 'any.file' into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"GROM\" (expected: \"FROM\")");

        assertParseError(null,
            "copy from into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"INTO\" (expected: \"[file name: string]\"");

        assertParseError(null,
            "copy from unquoted into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"UNQUOTED\" (expected: \"[file name: string]\"");

        assertParseError(null,
            "copy from unquoted.file into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"UNQUOTED\" (expected: \"[file name: string]\"");

        new SqlParser(null,
            "copy from '' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from 'd:/copy/from/into/format.csv' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from '/into' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from 'into' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        assertParseError(null,
            "copy from 'any.file' to Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"TO\" (expected: \"INTO\")");

        // Column list

        assertParseError(null,
            "copy from '" +
                "any.file' into Person () format csv",
            "Unexpected token: \")\" (expected: \"[identifier]\")");

        assertParseError(null,
            "copy from 'any.file' into Person (,) format csv",
            "Unexpected token: \",\" (expected: \"[identifier]\")");

        assertParseError(null,
            "copy from 'any.file' into Person format csv",
            "Unexpected token: \"FORMAT\" (expected: \"(\")");

        // FORMAT

        assertParseError(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName)",
            "Unexpected end of command (expected: \"FORMAT\")");

        assertParseError(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format lsd",
            "Unknown format name: LSD");

        // FORMAT CSV CHARSET

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'utf-8'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'UTF-8'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'UtF-8'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'windows-1251'")
            .nextCommand();

        new SqlParser(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset" +
            " 'ISO-2022-JP'")
            .nextCommand();

        assertParseError(null,
            "copy from 'any.file' into Person (_key, age, firstName, lastName) format csv charset ",
            "Unexpected end of command (expected: \"[string]\")");
    }
}
