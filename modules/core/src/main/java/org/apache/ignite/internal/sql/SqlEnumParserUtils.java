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

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;

public final class SqlEnumParserUtils {

    /** FIXME */
    public static <T extends Enum<T>> boolean tryParseEnumParam(SqlLexer lex, String keyword, Class<T> enumCls,
        @Nullable Set<String> parsedParams, boolean allowDflt, SqlParserUtils.Setter<T> setter) {

        if (SqlParserUtils.matchesKeyword(lex.lookAhead(), keyword)) {

            if (parsedParams != null && parsedParams.contains(keyword))
                throw error(lex.currentToken(), "Duplicate parameter: " + keyword);

            lex.shift();

            SqlParserUtils.skipOptionalEqSign(lex);

            ParsedEnum<T> val = parseEnumIfSpecified(lex, enumCls, allowDflt);

            switch (val.outcome()) {
                case PARSED:
                    setter.apply(val.value(), false, false);

                    break;

                case DEFAULT:
                    if (allowDflt) {
                        setter.apply(null, true, false);

                        break;
                    }
                    else {
                        assert false : "Internal error";

                        throw errorEnumValue(lex.lookAhead(), enumCls);
                    }

                default:
                    assert val.isError() || val.isMissing() : val;

                    throw errorEnumValue(lex.lookAhead(), enumCls);
            }
        }
        else {
            ParsedEnum<T> val = parseEnumIfSpecified(lex, enumCls, allowDflt);

            if (val.outcome != ParsedEnum.Outcome.PARSED)
                return false;

            setter.apply(val.value(), false, false);
        }

        if (parsedParams != null)
            parsedParams.add(keyword);

        return true;
    }

    /** FIXME */
    public static <T extends Enum<T>> T parseEnum(SqlLexer lex, Class<T> enumCls) {
        return parseEnum(lex, enumCls, true, null);
    }

    public static <T extends Enum<T>> T parseEnum(SqlLexer lex, Class<T> enumCls, T defaultVal) {
        return parseEnum(lex, enumCls, true, defaultVal);
    }

    public static <T extends Enum<T>> SqlParseException errorEnumValue(SqlLexerToken tok, Class<T> enumCls) {
        throw SqlParserUtils.errorUnexpectedToken(tok, "[" + enumCls.getCanonicalName()
            + " constant, one of " + Arrays.toString(enumCls.getEnumConstants()));
    }

    private static <T extends Enum<T>> T parseEnum(SqlLexer lex, Class<T> enumCls, boolean allowDflt, T defaultVal) {

        ParsedEnum<T> val = parseEnumIfSpecified(lex, enumCls, allowDflt);

        switch (val.outcome()) {
            case PARSED:
                return val.value();

            case DEFAULT:
                if (allowDflt)
                    return defaultVal;

                assert false : "Internal error";

                throw errorEnumValue(lex.lookAhead(), enumCls);

            default:
                assert val.isError() || val.isMissing();

                throw errorEnumValue(lex.lookAhead(), enumCls);
        }
    }

    /** FIXME */
    private static <T extends Enum<T>> ParsedEnum<T> parseEnumIfSpecified(SqlLexer lex, Class<T> enumCls,
        boolean allowDflt) {

        SqlLexerToken nextTok = lex.lookAhead();

        if (nextTok.tokenType() != SqlLexerTokenType.KEYWORD)
            return ParsedEnum.MISSING;

        if (allowDflt && nextTok.token().equals(SqlKeyword.DEFAULT)) {

            lex.shift();

            return ParsedEnum.DEFAULT;
        }

        try {
            T val = Enum.valueOf(enumCls, nextTok.token().trim().toUpperCase());

            lex.shift();

            return new ParsedEnum(val);
        }
        catch (IllegalArgumentException e) {

            return ParsedEnum.ERROR;
        }
    }

    public static class ParsedEnum<T extends Enum<T>> {
        public enum Outcome {
            MISSING,
            PARSED,
            DEFAULT,
            ERROR;
        }
        public final static ParsedEnum ERROR = new ParsedEnum(Outcome.ERROR);

        public final static ParsedEnum DEFAULT = new ParsedEnum(Outcome.DEFAULT);

        public final static ParsedEnum MISSING = new ParsedEnum(Outcome.MISSING);

        private final Outcome outcome;

        private final T value;
        private ParsedEnum(Outcome outcome) {
            this.outcome = outcome;
            this.value = null;
        }

        private ParsedEnum(T value) {
            this.outcome = Outcome.PARSED;
            this.value = value;
        }

        public Outcome outcome() {
            return outcome;
        }

        public T value() {
            assert outcome == Outcome.PARSED;
            return value;
        }

        public boolean isMissing() {
            return outcome == Outcome.MISSING;
        }

        public boolean isError() {
            return outcome == Outcome.ERROR;
        }

        public boolean isDefault() {
            return outcome == Outcome.DEFAULT;
        }

        public boolean isValue() {
            return outcome == Outcome.PARSED;
        }

        @Override public String toString() {
            return "ParsedEnum{outcome=" + outcome.name()
                + (isValue() ? ("; value=" + value()) : "" )
                + "}";
        }
    }

    /** FIXME */
    public static boolean tryParseBoolean(SqlLexer lex, String trueKeyword, String falseKeyword,
        @Nullable Set<String> parsedParams, boolean allowDflt, SqlParserUtils.Setter<Boolean> setter) {

        SqlLexerToken nextTok = lex.lookAhead();

        if (SqlParserUtils.matchesKeyword(nextTok, trueKeyword)) {
            lex.shift();

            if (parsedParams != null && parsedParams.contains(trueKeyword))
                throw error(lex.currentToken(), "Duplicate parameter: " + trueKeyword);

            if (lex.lookAhead().tokenType() == SqlLexerTokenType.EQUALS) {
                lex.shift();

                Boolean val = parseBoolean(lex, allowDflt);

                setter.apply(val, false, false);
            }
            else
                setter.apply(true, false, false);
        }
        else if (SqlParserUtils.matchesKeyword(nextTok, falseKeyword)) {

            if (parsedParams != null && parsedParams.contains(trueKeyword))
                throw error(lex.currentToken(), "Duplicate parameter: " + falseKeyword);

            setter.apply(false, false, false);

            lex.shift();
        }
        else
            return false;

        if (parsedParams != null)
            parsedParams.add(trueKeyword);

        return true;
    }

    /** FIXME */
    public static Boolean parseBoolean(SqlLexer lex, boolean allowDflt) {

        BooleanEnum val = parseEnum(lex, BooleanEnum.class, allowDflt, null);

        return val == null ? null : val.toBoolean();
    }

    /** FIXME */
    public enum BooleanEnum {

        /** FIXME */
        TRUE(true),

        /** FIXME */
        FALSE(false),

        /** FIXME */
        YES(true),

        /** FIXME */
        NO(false);

        /** FIXME */
        private final boolean val;

        /** FIXME */
        BooleanEnum(boolean val) {
            this.val = val;
        }

        /** FIXME */
        public boolean toBoolean() {
            return val;
        }
    }

    /** FIXME */
    private SqlEnumParserUtils() {
        // Noop.
    }
}
