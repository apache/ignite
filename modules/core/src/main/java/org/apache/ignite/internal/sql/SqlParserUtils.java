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
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.DEFAULT;
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
        SqlLexerToken tok = lex.lookAhead();

        if (matchesKeyword(tok, IF)) {
            lex.shift();

            skipKeyword(lex, EXISTS);

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
        SqlLexerToken tok = lex.lookAhead();

        if (matchesKeyword(tok, IF)) {
            lex.shift();

            skipKeyword(lex, NOT);
            skipKeyword(lex, EXISTS);

            return true;
        }

        return false;
    }

    /**
     * Skip comma or right parenthesis.
     *
     * @param lex The lexer.
     * @return The skipped token type.
     */
    public static SqlLexerTokenType skipCommaOrRightParenthesis(SqlLexer lex) {
        if (lex.shift()) {
            switch (lex.tokenType()) {
                case PARENTHESIS_RIGHT:
                case COMMA:
                    return lex.tokenType();

                default:
                    // Fall through
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

        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.KEYWORD) {
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
     * @param tok Token.
     * @return {@code True} if we are standing on possible identifier.
     */
    public static boolean isValidIdentifier(SqlLexerToken tok) {
        switch (tok.tokenType()) {
            case KEYWORD:
                char c = tok.tokenFirstChar();

                return ((c >= 'A' && c <= 'Z') || c == '_') && !SqlKeyword.isKeyword(tok.token());

            case DBL_QUOTED:
                return true;

            default:
                return false;
        }
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param tok Token..
     * @param expKeyword Expected keyword.
     * @return {@code True} if matches.
     */
    public static boolean matchesKeyword(SqlLexerToken tok, String expKeyword) {
        return tok.tokenType() == SqlLexerTokenType.KEYWORD && expKeyword.equals(tok.token());
    }

    /**
     * Skip token if it matches expected keyword.
     *
     * @param lex Lexer.
     * @param expKeyword Expected keyword.
     */
    public static void skipKeyword(SqlLexer lex, String expKeyword) {
        if (lex.shift() && matchesKeyword(lex, expKeyword))
            return;

        throw errorUnexpectedToken(lex, expKeyword);
    }

    /**
     * Skip next token if it matches expected type.
     *
     * @param lex Lexer.
     * @param tokTyp Expected token type.
     */
    public static void skipToken(SqlLexer lex, SqlLexerTokenType tokTyp) {
        if (lex.shift() && F.eq(lex.tokenType(), tokTyp))
            return;

        throw errorUnexpectedToken(lex, tokTyp.asString());
    }

    /** FIXME */
    public static boolean skipOptionalEqSign(SqlLexer lex) {
        if (lex.lookAhead().tokenType() == SqlLexerTokenType.EQUALS) {
            lex.shift();
            return true;
        }
        else
            return false;
    }

    /** FIXME */
    public static void parseKeywordOrQuoted(SqlLexer lex, String paramDesc, boolean isIdentifier, Setter<String> setter) {
        SqlLexerToken nextTok = lex.lookAhead();

        switch (nextTok.tokenType()) {
            case DBL_QUOTED:
                if (isIdentifier && !isValidIdentifier(nextTok))
                    throw errorUnexpectedToken(nextTok, "[optionally quoted identifier " + paramDesc + "]");

                lex.shift();

                setter.apply(lex.token(), false, true);

                return;

            case KEYWORD:
                if (isIdentifier && !isValidIdentifier(nextTok))
                    throw errorUnexpectedToken(nextTok, "[optionally quoted identifier " + paramDesc + "]");

                lex.shift();

                setter.apply(lex.token(), false, false);

                return;

            default:
                throw errorUnexpectedToken(nextTok, "[optionally quoted " + paramDesc + "]");
        }
    }

    /**
     * Create parse exception referring to current lexer position.
     *
     * @param tok Token.
     * @param msg Message.
     * @return Exception.
     */
    public static SqlParseException error(SqlLexerToken tok, String msg) {
        return error0(tok, IgniteQueryErrorCode.PARSING, msg);
    }

    /**
     * Create parse exception referring to current lexer position.
     *
     * @param tok Token.
     * @param code Error code.
     * @param msg Message.
     * @return Exception.
     */
    private static SqlParseException error0(SqlLexerToken tok, int code, String msg) {
        return new SqlParseException(tok.sql(), tok.tokenPosition(), code, msg);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param tok Token.
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexerToken tok) {
        return errorUnexpectedToken0(tok);
    }

    /**
     * Throw unsupported token exception if passed keyword is found.
     *
     * @param tok Token.
     * @param keyword Keyword.
     */
    public static void errorUnsupportedIfMatchesKeyword(SqlLexerToken tok, String keyword) {
        if (matchesKeyword(tok, keyword))
            throw errorUnsupported(tok);
    }

    /**
     * Throw unsupported token exception if one of passed keywords is found.
     *
     * @param tok Token.
     * @param keywords Keywords.
     */
    public static void errorUnsupportedIfMatchesKeyword(SqlLexerToken tok, String... keywords) {
        if (F.isEmpty(keywords))
            return;

        for (String keyword : keywords)
            errorUnsupportedIfMatchesKeyword(tok, keyword);
    }

    /**
     * Error on unsupported keyword.
     *
     * @param tok Token.
     * @return Error.
     */
    public static SqlParseException errorUnsupported(SqlLexerToken tok) {
        throw error0(tok, IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
            "Unsupported keyword: \"" + tok.token() + "\"");
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param lex Lexer.
     * @param expTok Expected token.
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexer lex, String expTok) {
        return errorUnexpectedToken0(lex, expTok);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param tok Token.
     * @param firstExpTok First expected token.
     * @param expTokens Additional expected tokens (if any).
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexerToken tok, String firstExpTok,
        String... expTokens) {
        if (F.isEmpty(expTokens))
            return errorUnexpectedToken0(tok, firstExpTok);
        else {
            String[] expTokens0 = new String[expTokens.length + 1];

            expTokens0[0] = firstExpTok;

            System.arraycopy(expTokens, 0, expTokens0, 1, expTokens.length);

            throw errorUnexpectedToken0(tok, expTokens0);
        }
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param tok Token.
     * @param expTokens Expected tokens (if any).
     * @return Exception.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private static SqlParseException errorUnexpectedToken0(SqlLexerToken tok, String... expTokens) {
        String token0 = tok.token();

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

        throw error(tok, msg.toString());
    }

    /** FIXME */
    public static boolean tryParseStringParam(SqlLexer lex, String keyword, String description,
        @Nullable Set<String> parsedParams, boolean isIdentifier, boolean allowDefault, Setter<String> setter) {

        if (!matchesKeyword(lex.lookAhead(), keyword))
            return false;

        if (parsedParams != null && parsedParams.contains(keyword))
            throw error(lex.currentToken(), "Duplicate parameter: " + keyword);

        lex.shift();

        skipOptionalEqSign(lex);

        if (allowDefault && matchesKeyword(lex.lookAhead(), DEFAULT)) {

            lex.shift();

            setter.apply(null, true, false);
        }
        else
            parseKeywordOrQuoted(lex, description, isIdentifier, setter);

        if (parsedParams != null)
            parsedParams.add(keyword);

        return true;
    }

    /** FIXME */
    public static boolean tryParseIntParam(SqlLexer lex, String keyword, @Nullable Set<String> parsedParams,
        boolean allowDefault, Setter<Integer> setter) {

        if (!matchesKeyword(lex.lookAhead(), keyword))
            return false;

        if (parsedParams != null && parsedParams.contains(keyword))
            throw error(lex.currentToken(), "Only one " + keyword + " clause may be specified.");

        lex.shift();

        skipOptionalEqSign(lex);

        if (allowDefault && matchesKeyword(lex.lookAhead(), DEFAULT)) {

            lex.shift();

            setter.apply(null, true, false);
        }
        else
            setter.apply(parseInt(lex), false, false);

        if (parsedParams != null)
            parsedParams.add(keyword);

        return true;
    }

    /** FIXME */
    public interface Setter<T> {

        /** FIXME */
        void apply(T val, boolean isDflt, boolean isQuoted);
    }

    /**
     * Private constructor.
     */
    private SqlParserUtils() {
        // No-op.
    }
}
