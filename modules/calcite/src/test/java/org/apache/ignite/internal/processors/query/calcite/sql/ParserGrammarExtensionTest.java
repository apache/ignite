/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.ignite.internal.processors.query.calcite.sql;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.test.EchoTestSqlParserImpl;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Verifies that a test-only grammar can extend the complete Ignite SQL parser. */
public class ParserGrammarExtensionTest {
    /** Verifies the extension command and syntax inherited from Calcite and Ignite. */
    @Test public void testEchoExtendsIgniteGrammar() {
        SqlParser.Config parserCfg = CalciteQueryProcessor.FRAMEWORK_CONFIG.getParserConfig()
            .withParserFactory(EchoTestSqlParserImpl.FACTORY);

        SqlNodeList echo = Commons.parse("ECHO 1 + 2", parserCfg);
        SqlNodeList select = Commons.parse("SELECT 1", parserCfg);
        SqlNodeList savepoint = Commons.parse("SAVEPOINT grammar_test", parserCfg);

        assertEquals(1, echo.size());
        assertEquals(SqlKind.SELECT, echo.get(0).getKind());
        assertEquals("SELECT 1 + 2", echo.get(0).toString());
        assertEquals(1, select.size());
        assertEquals(1, savepoint.size());
    }
}
