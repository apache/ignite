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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;

/** A set of tests for the common SqlParser syntax, regardless of specific commands. */
public class SqlParserSelfTest extends GridCommonAbstractTest {

    /** Verifies that single quotes are handled correctly. */
    public void testSingleQuotes() {

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

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex2.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
                assertEquals(lex2.lookAhead().token(), "''");

                return null;
            }
        }, SqlParseException.class, "Unclosed quoted identifier.");
    }

    /** Verifies that double quotes are handled correctly. */
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

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex2.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
                assertEquals(lex2.lookAhead().token(), "\"\"");

                return null;
            }
        }, SqlParseException.class, "Unclosed quoted identifier.");

        final SqlLexer lex3 = new SqlLexer("\"quoted \\\"\" text\"");

        assertEquals(lex3.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex3.lookAhead().token(), "quoted \"");

        lex3.shift();

        assertEquals(lex3.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
        assertEquals(lex3.lookAhead().token(), "TEXT");

        lex3.shift();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex3.lookAhead().tokenType(), SqlLexerTokenType.KEYWORD);
                assertEquals(lex3.lookAhead().token(), "text");

                return null;
            }
        }, SqlParseException.class, "Unclosed quoted identifier.");
    }

    /** Verifies that escape sequences are handled correctly. */
    public void testEscapeSeqs() {
        checkEscapeSeq("\"\\b\"", "\b");
        checkEscapeSeq("\"\\f\"", "\f");
        checkEscapeSeq("\"\\n\"", "\n");
        checkEscapeSeq("\"\\t\"", "\t");
        checkEscapeSeq("\"\\r\"", "\r");
        checkEscapeSeq("\"\\Z\"", "\032");
        checkEscapeSeq("\"\\\\\"", "\\");

        checkEscapeSeq("\"\\0\"", "\0");
        checkEscapeSeq("\"\\0 \"", "\0 ");
        checkEscapeSeq("\"\\7 \"", "\7 ");
        checkEscapeSeq("\"\\78\"", "\78");
        checkEscapeSeq("\"\\377\"", "" + '\377');
        checkEscapeSeq("\"\\477\"", "" + '\47' + '7');
        checkEscapeSeq("\"\\0078\"", "\78");
        checkEscapeSeq("\"\\008\"", "\08");
        checkEscapeSeq("\"\\08\"", "\08");

        checkEscapeSeq("\"\\8\"", "8");

        checkInvalidEscapeSeq("\"\\x\"", "Character cannot be part of escape sequence: '\"'");

        checkEscapeSeq("\"\\x0\"", "\0");
        checkEscapeSeq("\"\\x0 \"", "\0 ");
        checkEscapeSeq("\"\\x00 \"", "\0 ");
        checkEscapeSeq("\"\\x0g\"", "\0g");

        checkInvalidEscapeSeq("\"\\xg\"", "Character cannot be part of escape sequence: 'g'");

        checkEscapeSeq("\"\\xff \"", "\u00ff ");

        checkInvalidEscapeSeq("\"\\uf\"", "Character cannot be part of escape sequence: '\"'");
        checkInvalidEscapeSeq("\"\\uff\"", "Character cannot be part of escape sequence: '\"'");
        checkInvalidEscapeSeq("\"\\ufff\"", "Character cannot be part of escape sequence: '\"'");

        checkEscapeSeq("\"\\uffff \"", "\uffff" + " ");
        checkEscapeSeq("\"\\ufffff \"", "\uffff" + "f ");
        checkEscapeSeq("\"\\uffffff \"", "\uffff" + "ff ");

        checkInvalidEscapeSeq("\"\\ug\"", "Character cannot be part of escape sequence: 'g'");
        checkInvalidEscapeSeq("\"\\ufg\"", "Character cannot be part of escape sequence: 'g'");
        checkInvalidEscapeSeq("\"\\uffg\"", "Character cannot be part of escape sequence: 'g'");
        checkInvalidEscapeSeq("\"\\ufffg\"", "Character cannot be part of escape sequence: 'g'");
    }

    /**
     * Checks that an escape sequence in handled correctly.
     * @param sql SQL part with a double-quoted string.
     * @param convertedTok A string to which this double-quoted token should be converted to by SQL parser.
     */
    private void checkEscapeSeq(String sql, String convertedTok) {
        SqlLexer lex = new SqlLexer(sql);

        assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);
        assertEquals(lex.lookAhead().token(), convertedTok);
    }

    /**
     * Checks that parsing an escape sequence terminates with an exception.
     * @param sql SQL part with a double-quoted string.
     * @param errorMsg Error message to expect (see
     *      {@link GridTestUtils#assertThrows(IgniteLogger, Callable, Class, String)} for details).
     */
    private void checkInvalidEscapeSeq(String sql, String errorMsg) {
        final SqlLexer lex = new SqlLexer(sql);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertEquals(lex.lookAhead().tokenType(), SqlLexerTokenType.DBL_QUOTED);

                return null;
            }
        }, SqlParseException.class, errorMsg);
    }
}
