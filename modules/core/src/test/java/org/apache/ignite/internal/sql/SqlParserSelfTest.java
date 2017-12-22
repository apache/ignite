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

import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.StrOrRegex;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;

/** FIXME */
public class SqlParserSelfTest extends GridCommonAbstractTest {

    /** FIXME */
    public void testSingleQuotess() {

        SqlLexer lex = new SqlLexer("'quoted text'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted text");

        lex = new SqlLexer("'quoted \"text\"'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \"text\"");

        lex = new SqlLexer("'quoted '' text'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted ' text");

        lex = new SqlLexer("'quoted \\\\ text'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \\\\ text");

        lex = new SqlLexer("'quoted \\n text'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \\n text");

        lex = new SqlLexer("'quoted \\\" text'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \\\" text");

        lex = new SqlLexer("'quoted \\'' text'");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \\' text");

        lex = new SqlLexer("''''''");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.SGL_QUOTED);
        assertEquals(lex.lookAhead().token(), "''");

        final SqlLexer lex2 = new SqlLexer("'''''");

        GridTestUtils.assertThrowsRe(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex2.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
                assertEquals(lex2.lookAhead().token(), "''");

                return null;
            }
        }, SqlParseException.class, StrOrRegex.of("Unclosed quoted identifier."));
    }

    public void testDoubleQuotes() {
        SqlLexer lex = new SqlLexer("\"quoted text\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted text");

        lex = new SqlLexer("\"quoted 'text'\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted 'text'");

        lex = new SqlLexer("\"quoted \"\" text\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \" text");

        lex = new SqlLexer("\"quoted \\\\ text\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \\ text");

        lex = new SqlLexer("\"quoted \\n text\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \n text");

        lex = new SqlLexer("\"quoted \\\" text\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted \" text");

        lex = new SqlLexer("\"quoted \\'\\\\\\\" text\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "quoted '\\\" text");

        lex = new SqlLexer("\"\"\"\"\"\"");

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), "\"\"");

        final SqlLexer lex2 = new SqlLexer("\"\"\"\"\"");

        GridTestUtils.assertThrowsRe(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex2.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
                assertEquals(lex2.lookAhead().token(), "\"\"");

                return null;
            }
        }, SqlParseException.class, StrOrRegex.of("Unclosed quoted identifier."));

        final SqlLexer lex3 = new SqlLexer("\"quoted \\\"\" text\"");

        assertEquals(lex3.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex3.lookAhead().token(), "quoted \"");

        lex3.shift();

        assertEquals(lex3.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
        assertEquals(lex3.lookAhead().token(), "TEXT");

        lex3.shift();

        GridTestUtils.assertThrowsRe(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex3.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
                assertEquals(lex3.lookAhead().token(), "text");

                return null;
            }
        }, SqlParseException.class, StrOrRegex.of("Unclosed quoted identifier."));
    }

    /** FIXME */
    public void testEscapeSeqs() {
        checkEscapeSeq("\"\\0\"", "\0");
        checkEscapeSeq("\"\\b\"", "\b");
        checkEscapeSeq("\"\\f\"", "\f");
        checkEscapeSeq("\"\\n\"", "\n");
        checkEscapeSeq("\"\\t\"", "\t");
        checkEscapeSeq("\"\\\\\"", "\\");
        checkEscapeSeq("\"\\r\"", "\r");
        checkEscapeSeq("\"\\Z\"", "\032");
    }

    /** FIXME */
    private void checkEscapeSeq(String sql, String convertedToken) {
        SqlLexer lex = new SqlLexer(sql);

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), convertedToken);
    }
}
