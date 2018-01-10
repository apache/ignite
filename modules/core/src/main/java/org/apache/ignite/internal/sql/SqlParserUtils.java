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

import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.EXISTS;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.NOT;
import static org.apache.ignite.internal.sql.SqlLexerTokenType.COMMA;
import static org.apache.ignite.internal.sql.SqlLexerTokenType.PARENTHESIS_RIGHT;

/**
 * Parser utility methods.
 */
public class SqlParserUtils {

    public enum ParamFormat {
        KEY_SPACE_VAL, KEY_EQ_VAL
    }

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

            skipKeywords(lex, EXISTS);

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

            skipKeywords(lex, NOT);
            skipKeywords(lex, EXISTS);

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

        throw errorUnexpectedToken(lex, COMMA.asString(), PARENTHESIS_RIGHT.asString());
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
     * Skip token if it matches expected keyword sequence.
     *
     * @param lex Lexer.
     * @param expKeywords Expected keywords
     */
    public static void skipKeywords(SqlLexer lex, String... expKeywords) {
        for (String keyword : expKeywords) {
            if (!lex.shift() || !matchesKeyword(lex, keyword))
                throw errorUnexpectedToken(lex, keyword);
        }
    }

    /**
     * Skips a token type or throws an error if the current token type is different.
     *
     * @param lex The lexer.
     * @param tokTyp Token type to skip.
     */
    public static void skipToken(SqlLexer lex, SqlLexerTokenType tokTyp) {
        if (lex.shift() && lex.tokenType() == tokTyp)
            return;

        throw errorUnexpectedToken(lex, tokTyp.asString());
    }

    /**
     * Skips token if it matches {@code tokTyp} parameter, otherwise does nothing.
     *
     * @param lex The lexer.
     * @param tokTyp Token type to skip.
     * @return true if the specified token met and skipped, false if the token at the current position is different.
     */
    public static boolean skipOptionalToken(SqlLexer lex, SqlLexerTokenType tokTyp) {
        if (lex.lookAhead().tokenType() == tokTyp) {
            lex.shift();
            return true;
        }
        else
            return false;
    }

    /**
     * Skips next token if it's an equal sign.
     *
     * @param lex Lexer.
     */
    public static boolean skipOptionalEqSign(SqlLexer lex) {
        return skipOptionalToken(lex, SqlLexerTokenType.EQUALS);
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

    public static String parseString(SqlLexer lex) {
        if (lex.shift() &&
            (lex.tokenType() == SqlLexerTokenType.DEFAULT || lex.tokenType() == SqlLexerTokenType.QUOTED))
            return lex.token();

        throw errorUnexpectedToken(lex, "[string]");
    }

    public static ParamFormat checkAndSkipParamName(SqlLexer lex, Set<String> parsedParams) {
        lex.shift();

        String paramName = lex.token();

        if (!parsedParams.add(paramName))
            throw error(lex, "Only one " + paramName + " clause may be specified.");

        return skipOptionalToken(lex, SqlLexerTokenType.EQUALS) ? ParamFormat.KEY_EQ_VAL : ParamFormat.KEY_SPACE_VAL;
    }

    public static <T extends Enum<T>> T parseEnum(SqlLexer lex, Class<T> enumCls) {
        lex.shift();

        try {
            return Enum.valueOf(enumCls, lex.token().trim().toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw errorUnexpectedToken(lex, "[" + enumValuesList(enumCls) + "]");
        }
    }

    private static <E extends Enum<E>> String enumValuesList(Class<E> enumCls) {

        E[] enumVals = enumCls.getEnumConstants();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < enumVals.length - 2; i++)
            sb.append(String.valueOf(enumVals[i])).append(", ");

        sb.append(", or ").append(enumVals[enumVals.length - 1]);

        return sb.toString();
    }

    public static Boolean tryParseBoolean(SqlLexer lex, ParamFormat paramFmt) {
        Boolean val = null;

        SqlLexerToken token = lex.lookAhead();

        if (token.tokenType() == SqlLexerTokenType.QUOTED || token.tokenType() == SqlLexerTokenType.DEFAULT) {
            String valStr = token.token().toUpperCase();

            if (F.eq(valStr, "TRUE") || F.eq(valStr, "1"))
                val = true;
            else if (F.eq(valStr, "FALSE") || F.eq(valStr, "0"))
                val = false;

            if (val != null)
                lex.shift();
        }

        if (val == null && paramFmt == ParamFormat.KEY_EQ_VAL)
            throw errorUnexpectedToken(lex, "[boolean]");

        return val;
    }

    /**
     * Private constructor.
     */
    private SqlParserUtils() {
        // No-op.
    }
}
