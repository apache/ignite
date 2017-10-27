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

import org.apache.ignite.internal.sql.command.SqlQualifiedName;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Parser utility methods.
 */
public class SqlParserUtils {
    /**
     * Process name.
     *
     * @param lex Lexer.
     * @param additionalExpTokens Additional expected tokens in case of error.
     * @return Name.
     */
    public static String parseIdentifier(SqlLexer lex, String... additionalExpTokens) {
        if (lex.shift() && isIdentifier(lex))
            return lex.token();

        throw errorUnexpectedToken(lex, "[identifier]", additionalExpTokens);
    }

    /**
     * Process qualified name.
     *
     * @param lex Lexer.
     * @param additionalExpTokens Additional expected tokens in case of error.
     * @return Qualified name.
     */
    public static SqlQualifiedName parseQualifiedIdentifier(SqlLexer lex, String... additionalExpTokens) {
        if (lex.shift() && isIdentifier(lex)) {
            SqlQualifiedName res = new SqlQualifiedName();

            String first = lex.token();

            SqlParserToken nextToken = lex.lookAhead();

            if (nextToken != null && nextToken.tokenType() == SqlLexerTokenType.DOT) {
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
    private static boolean isIdentifier(SqlParserToken token) {
        switch (token.tokenType()) {
            case DEFAULT:
                char c = token.tokenFirstChar();

                if ((c >= 'A' && c <= 'Z') || c == '_')
                    return true;

                throw error(token, "Illegal identifier name.");

            case QUOTED:
                return true;

            default:
                return false;
        }
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param lex Lexer.
     * @param expKeyword Expected keyword.
     * @return {@code True} if matches.
     */
    public static boolean matchesKeyword(SqlParserToken lex, String expKeyword) {
        if (lex.tokenType() != SqlLexerTokenType.DEFAULT)
            return false;

        String token = lex.token();

        return expKeyword.equals(token);
    }

    /**
     * Skip token if it matches expected keyword.
     *
     * @param lex Lexer.
     * @param expKeyword Expected keyword.
     */
    public static void skipIfMatchesKeyword(SqlLexer lex, String expKeyword) {
        if (lex.shift() && SqlParserUtils.matchesKeyword(lex, expKeyword))
            return;

        throw errorUnexpectedToken(lex, expKeyword);
    }

    /**
     * Create parse exception referring to current lexer position.
     *
     * @param token Token.
     * @param msg Message.
     * @return Exception.
     */
    public static SqlParseException error(SqlParserToken token, String msg) {
        return new SqlParseException(token.sql(), token.tokenPosition(), msg);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param lex Lexer.
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexer lex) {
        return errorUnexpectedToken0(lex);
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
     * @param lex Lexer.
     * @param firstExpToken First expected token.
     * @param expTokens Additional expected tokens (if any).
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexer lex, String firstExpToken, String... expTokens) {
        if (F.isEmpty(expTokens))
            return errorUnexpectedToken0(lex, firstExpToken);
        else {
            String[] expTokens0 = new String[expTokens.length + 1];

            expTokens0[0] = firstExpToken;

            System.arraycopy(expTokens, 0, expTokens0, 1, expTokens.length);

            throw errorUnexpectedToken0(lex, expTokens0);
        }
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param lex Lexer.
     * @param expTokens Expected tokens (if any).
     * @return Exception.
     */
    private static SqlParseException errorUnexpectedToken0(SqlLexer lex, String... expTokens) {
        String token = lex.token();

        StringBuilder msg = new StringBuilder(
            token == null ? "Unexpected end of command" : "Unexpected token: " + token);

        if (!F.isEmpty(expTokens)) {
            msg.append(" (expected: ");

            boolean first = true;

            for (String expToken : expTokens) {
                if (first)
                    first = false;
                else
                    msg.append(", ");

                msg.append(expToken);
            }

            msg.append(")");
        }

        throw error(lex, msg.toString());
    }

    /**
     * Private constructor.
     */
    private SqlParserUtils() {
        // No-op.
    }
}
