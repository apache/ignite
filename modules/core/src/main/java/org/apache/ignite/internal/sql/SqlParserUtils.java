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

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.sql.command.SqlQualifiedName;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.sql.SqlKeyword.EXISTS;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.NOT;

/**
 * Parser utility methods.
 */
public class SqlParserUtils {
    /**
     * Parse IF EXISTS statement.
     *
     * @param lex Lexer.
     * @return {@code True} if statement is found.
     */
    public static boolean parseIfExists(SqlLexer lex) {
        SqlLexerToken token = lex.lookAhead();

        if (matchesKeyword(token, IF)) {
            lex.shift();

            skipIfMatchesKeyword(lex, EXISTS);

            return true;
        }

        return false;
    }

    /**
     * Parse IF NOT EXISTS statement.
     *
     * @param lex Lexer.
     * @return {@code True} if statement is found.
     */
    public static boolean parseIfNotExists(SqlLexer lex) {
        SqlLexerToken token = lex.lookAhead();

        if (matchesKeyword(token, IF)) {
            lex.shift();

            skipIfMatchesKeyword(lex, NOT);
            skipIfMatchesKeyword(lex, EXISTS);

            return true;
        }

        return false;
    }

    /**
     * Skip comma or right parenthesis.
     *
     * @param lex Lexer.
     * @return {@code True} if right parenthesis is found.
     */
    public static boolean skipCommaOrRightParenthesis(SqlLexer lex) {
        if (lex.shift()) {
            switch (lex.tokenType()) {
                case COMMA:
                    return false;

                case PARENTHESIS_RIGHT:
                    return true;
            }
        }

        throw errorUnexpectedToken(lex, ",", ")");
    }

    /**
     * Parse integer value (positive or negative).
     *
     * @param lex Lexer.
     * @return Integer value.
     */
    public static int parseInt(SqlLexer lex) {
        int sign = 1;

        if (lex.lookAhead().tokenType() == SqlLexerTokenType.MINUS) {
            sign = -1;

            lex.shift();
        }

        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            try {
                long val = sign * Long.parseLong(lex.token());

                if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE)
                    return (int)val;

                // Fall through.
            }
            catch (NumberFormatException e) {
                // Fall through.
            }
        }

        throw errorUnexpectedToken(lex, "[integer]");
    }

    /**
     * Parse boolean parameter value based on presence of tokens 1, 0, ON, OFF. Not that this is not
     * and is not intended to be routine for parsing a boolean literal from TRUE/FALSE.
     * @param lex Lexer.
     * @return Boolean parameter value.
     */
    public static boolean parseBoolean(SqlLexer lex) {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.token()) {
                case SqlKeyword.ON:
                case "1":
                    return true;

                case SqlKeyword.OFF:
                case "0":
                    return false;
            }
        }

        throw errorUnexpectedToken(lex, SqlKeyword.ON, SqlKeyword.OFF, "1", "0");
    }

    /**
     * Process name.
     *
     * @param lex Lexer.
     * @param additionalExpTokens Additional expected tokens in case of error.
     * @return Name.
     */
    public static String parseIdentifier(SqlLexer lex, String... additionalExpTokens) {
        if (lex.shift() && isValidIdentifier(lex))
            return lex.token();

        throw errorUnexpectedToken(lex, "[identifier]", additionalExpTokens);
    }

    /**
     * Process name.
     *
     * @param lex Lexer.
     * @param additionalExpTokens Additional expected tokens in case of error.
     * @return Name.
     */
    public static String parseUsername(SqlLexer lex, String... additionalExpTokens) {
        if (lex.shift() && isValidIdentifier(lex))
            return lex.token();

        throw errorUnexpectedToken(lex, "[username identifier]", additionalExpTokens);
    }

    /**
     * Process name.
     *
     * @param lex Lexer.
     * @param additionalExpTokens Additional expected tokens in case of error.
     * @return Name.
     */
    public static String parseString(SqlLexer lex, String... additionalExpTokens) {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.STRING)
            return lex.token();

        throw errorUnexpectedToken(lex, "[string]", additionalExpTokens);
    }

    /**
     * Process qualified name.
     *
     * @param lex Lexer.
     * @param additionalExpTokens Additional expected tokens in case of error.
     * @return Qualified name.
     */
    public static SqlQualifiedName parseQualifiedIdentifier(SqlLexer lex, String... additionalExpTokens) {
        if (lex.shift() && isValidIdentifier(lex)) {
            SqlQualifiedName res = new SqlQualifiedName();

            String first = lex.token();

            SqlLexerToken nextTok = lex.lookAhead();

            if (nextTok.tokenType() == SqlLexerTokenType.DOT) {
                lex.shift();

                String second = parseIdentifier(lex);

                return res.schemaName(first).name(second);
            }
            else
                return res.name(first);
        }

        throw errorUnexpectedToken(lex, "[qualified identifier]", additionalExpTokens);
    }

    /**
     * Check if token is identifier.
     *
     * @param token Token.
     * @return {@code True} if we are standing on possible identifier.
     */
    public static boolean isValidIdentifier(SqlLexerToken token) {
        switch (token.tokenType()) {
            case DEFAULT:
                char c = token.tokenFirstChar();

                return ((c >= 'A' && c <= 'Z') || c == '_') && !SqlKeyword.isKeyword(token.token());

            case QUOTED:
                return true;

            default:
                return false;
        }
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param token Token..
     * @param expKeyword Expected keyword.
     * @return {@code True} if matches.
     */
    public static boolean matchesKeyword(SqlLexerToken token, String expKeyword) {
        return token.tokenType() == SqlLexerTokenType.DEFAULT && expKeyword.equals(token.token());
    }

    /**
     * Skip token if it matches expected keyword.
     *
     * @param lex Lexer.
     * @param expKeyword Expected keyword.
     */
    public static void skipIfMatchesKeyword(SqlLexer lex, String expKeyword) {
        if (lex.shift() && matchesKeyword(lex, expKeyword))
            return;

        throw errorUnexpectedToken(lex, expKeyword);
    }

    /**
     * Skip token if it matches expected keyword by using lookahead.
     * If next token is not what we expect, no shift is done.
     *
     * @param lex Lexer.
     * @param expKeyword Expected keyword.
     */
    static void skipIfMatchesOptionalKeyword(SqlLexer lex, String expKeyword) {
        SqlLexerToken nextTok = lex.lookAhead();

        if (matchesKeyword(nextTok, expKeyword))
            lex.shift();
    }

    /**
     * Skip next token if it matches expected type.
     *
     * @param lex Lexer.
     * @param tokenTyp Expected token type.
     */
    public static void skipIfMatches(SqlLexer lex, SqlLexerTokenType tokenTyp) {
        if (lex.shift() && F.eq(lex.tokenType(), tokenTyp))
            return;

        throw errorUnexpectedToken(lex, tokenTyp.asString());
    }

    /**
     * Create parse exception referring to current lexer position.
     *
     * @param token Token.
     * @param msg Message.
     * @return Exception.
     */
    public static SqlParseException error(SqlLexerToken token, String msg) {
        return error0(token, IgniteQueryErrorCode.PARSING, msg);
    }

    /**
     * Create parse exception referring to current lexer position.
     *
     * @param token Token.
     * @param code Error code.
     * @param msg Message.
     * @return Exception.
     */
    private static SqlParseException error0(SqlLexerToken token, int code, String msg) {
        return new SqlParseException(token.sql(), token.tokenPosition(), code, msg);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param token Token.
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexerToken token) {
        return errorUnexpectedToken0(token);
    }

    /**
     * Throw unsupported token exception if passed keyword is found.
     *
     * @param token Token.
     * @param keyword Keyword.
     */
    public static void errorUnsupportedIfMatchesKeyword(SqlLexerToken token, String keyword) {
        if (matchesKeyword(token, keyword))
            throw errorUnsupported(token);
    }

    /**
     * Throw unsupported token exception if one of passed keywords is found.
     *
     * @param token Token.
     * @param keywords Keywords.
     */
    public static void errorUnsupportedIfMatchesKeyword(SqlLexerToken token, String... keywords) {
        if (F.isEmpty(keywords))
            return;

        for (String keyword : keywords)
            errorUnsupportedIfMatchesKeyword(token, keyword);
    }

    /**
     * Error on unsupported keyword.
     *
     * @param token Token.
     * @return Error.
     */
    public static SqlParseException errorUnsupported(SqlLexerToken token) {
        throw error0(token, IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported keyword: \"" + token.token() + "\"");
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param lex Lexer.
     * @param expToken Expected token.
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexer lex, String expToken) {
        return errorUnexpectedToken0(lex, expToken);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param token Token.
     * @param firstExpToken First expected token.
     * @param expTokens Additional expected tokens (if any).
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexerToken token, String firstExpToken,
        String... expTokens) {
        if (F.isEmpty(expTokens))
            return errorUnexpectedToken0(token, firstExpToken);
        else {
            String[] expTokens0 = new String[expTokens.length + 1];

            expTokens0[0] = firstExpToken;

            System.arraycopy(expTokens, 0, expTokens0, 1, expTokens.length);

            throw errorUnexpectedToken0(token, expTokens0);
        }
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param token Token.
     * @param expTokens Expected tokens (if any).
     * @return Exception.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private static SqlParseException errorUnexpectedToken0(SqlLexerToken token, String... expTokens) {
        String token0 = token.token();

        StringBuilder msg = new StringBuilder(
            token0 == null ? "Unexpected end of command" : "Unexpected token: \"" + token0 + "\"");

        if (!F.isEmpty(expTokens)) {
            msg.append(" (expected: ");

            boolean first = true;

            for (String expToken : expTokens) {
                if (first)
                    first = false;
                else
                    msg.append(", ");

                msg.append("\"" + expToken + "\"");
            }

            msg.append(")");
        }

        throw error(token, msg.toString());
    }

    /**
     * Private constructor.
     */
    private SqlParserUtils() {
        // No-op.
    }
}
