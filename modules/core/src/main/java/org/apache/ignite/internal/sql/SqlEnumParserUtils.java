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

/** FIXME */
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

    /** FIXME */
    public static <T extends Enum<T>> T parseEnum(SqlLexer lex, Class<T> enumCls, T defaultVal) {
        return parseEnum(lex, enumCls, true, defaultVal);
    }

    /** FIXME */
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

    /** FIXME */
    public static class ParsedEnum<T extends Enum<T>> {
        /** FIXME */
        public enum Outcome {
            /** FIXME */
            MISSING,
            /** FIXME */
            PARSED,
            /** FIXME */
            DEFAULT,
            /** FIXME */
            ERROR;
        }

        /** FIXME */
        public final static ParsedEnum ERROR = new ParsedEnum(Outcome.ERROR);

        /** FIXME */
        public final static ParsedEnum DEFAULT = new ParsedEnum(Outcome.DEFAULT);

        /** FIXME */
        public final static ParsedEnum MISSING = new ParsedEnum(Outcome.MISSING);

        /** FIXME */
        private final Outcome outcome;

        /** FIXME */
        private final T value;

        /** FIXME */
        private ParsedEnum(Outcome outcome) {
            this.outcome = outcome;
            this.value = null;
        }

        /** FIXME */
        private ParsedEnum(T value) {
            this.outcome = Outcome.PARSED;
            this.value = value;
        }

        /** FIXME */
        public Outcome outcome() {
            return outcome;
        }

        /** FIXME */
        public T value() {
            assert outcome == Outcome.PARSED;
            return value;
        }

        /** FIXME */
        public boolean isMissing() {
            return outcome == Outcome.MISSING;
        }

        /** FIXME */
        public boolean isError() {
            return outcome == Outcome.ERROR;
        }

        /** FIXME */
        public boolean isDefault() {
            return outcome == Outcome.DEFAULT;
        }

        /** FIXME */
        public boolean isValue() {
            return outcome == Outcome.PARSED;
        }

        /** {@inheritDoc} */
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

    /**
     * Parses a boolean value allownig additional values defined in {@link BooleanEnum} and possible default value.
     *
     * @param lex The lexer.
     * @param allowDflt Allow {@code DEFAULT} keyword to be used instead of boolean value.
     */
    public static Boolean parseBoolean(SqlLexer lex, boolean allowDflt) {

        BooleanEnum val = parseEnum(lex, BooleanEnum.class, allowDflt, null);

        return val == null ? null : val.toBoolean();
    }

    /** Enum listing boolean values with additional values like YES and NO. */
    public enum BooleanEnum {

        /** true. */
        TRUE(true),

        /** false. */
        FALSE(false),

        /** yes (true). */
        YES(true),

        /** no (false). */
        NO(false);

        /** Boolean value of the enum. */
        private final boolean val;

        /**
         * Creates an enum value.
         *
         * @param val Boolean value for this enum value.
         */
        BooleanEnum(boolean val) {
            this.val = val;
        }

        /**
         * Converts enum value to boolean.
         *
         * @return Boolean value of the enum.
         */
        public boolean toBoolean() {
            return val;
        }
    }

    /** Deprecates creating of an object of this type. */
    private SqlEnumParserUtils() {
        // Noop.
    }
}
