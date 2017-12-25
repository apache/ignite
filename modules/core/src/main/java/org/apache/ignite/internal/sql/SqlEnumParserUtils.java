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

/** The class contains various methods for parsing enum-based SQL options. */
public final class SqlEnumParserUtils {

    /**
     * Tries to parse enum parameter, if exists.
     *
     * <p>The parameter can be defined in one of the following ways:
     * {@code keyword <space> value},
     * {@code keyword=value}, and
     * {@code value}.
     *
     * <p>The values are taken from enum class. If {@code allowDflt} is true,
     * {@link SqlKeyword#DEFAULT} value is permitted.
     *
     * @param lex The lexer.
     * @param keyword The keyword, which prefixes
     * @param enumCls The enum class.
     * @param parsedParams The parameters, which has been already parsed for duplicate checking. If provided,
     *      a duplicate parameter is not allowed.
     * @param allowDflt Allow {@link SqlKeyword#DEFAULT} as the possible value.
     * @param setter A closure/lambda that is called with the value to set and some options.
     */
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

                        throw errorInEnumValue(lex.lookAhead(), enumCls, allowDflt);
                    }

                default:
                    assert val.isError() || val.isMissing() : val;

                    throw errorInEnumValue(lex.lookAhead(), enumCls, allowDflt);
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

    /**
     * Creates an exception to throw when value supplied in SQL does not correspond to any of the enum values.
     * The exception message contains the list of possible values.
     *
     * @param tok Token to supply to {@link SqlParseException} constructor.
     * @param enumCls The enum class.
     * @param allowDflt Add 'DEFAULT' keyword to the list of possible values.
     * @param <T> The enum class.
     * @return {@link SqlParseException} to throw.
     */
    public static <T extends Enum<T>> SqlParseException errorInEnumValue(SqlLexerToken tok, Class<T> enumCls,
        boolean allowDflt) {

        String expVals = Arrays.toString(enumCls.getEnumConstants());

        expVals = expVals.substring(1, expVals.length() - 1);

        if (allowDflt)
            expVals += ", " + SqlKeyword.DEFAULT;

        return SqlParserUtils.errorUnexpectedToken(tok, "[one of: " + expVals + "]");
    }

    /**
     * Parses enum value.
     *
     * @param lex The lexer.
     * @param enumCls The enum class.
     * @param allowDflt Allow specifying {@link SqlKeyword#DEFAULT} value.
     * @return Parsed enum value.
     * @throws SqlParseException When the value is not recognized.
     */
    private static <T extends Enum<T>> T parseEnum(SqlLexer lex, Class<T> enumCls, boolean allowDflt, T defaultVal) {

        ParsedEnum<T> val = parseEnumIfSpecified(lex, enumCls, allowDflt);

        switch (val.outcome()) {
            case PARSED:
                return val.value();

            case DEFAULT:
                if (allowDflt)
                    return defaultVal;

                assert false : "Internal error";

                throw errorInEnumValue(lex.lookAhead(), enumCls, allowDflt);

            default:
                assert val.isError() || val.isMissing();

                throw errorInEnumValue(lex.lookAhead(), enumCls, allowDflt);
        }
    }

    /**
     * Parses enum value.
     *
     * @param lex The lexer.
     * @param enumCls The enum class.
     * @param allowDflt Allow specifying {@link SqlKeyword#DEFAULT} value.
     * @return Parse result.
     */
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

    /**
     * Represents result of parsing an enum parameter.
     *
     * <p>When {@link Outcome} is {@link Outcome#PARSED}, contains the parsed value.</p>
     * */
    public static class ParsedEnum<T extends Enum<T>> {
        /** Parsing outcome. */
        public enum Outcome {
            /** Parameter is missing. */
            MISSING,
            /** Parameter has been parsed. */
            PARSED,
            /** Parameter value is default. */
            DEFAULT,
            /** Error encountered when parsing the enum. */
            ERROR;
        }

        /** Error singleton value of {@link ParsedEnum}. */
        public final static ParsedEnum ERROR = new ParsedEnum(Outcome.ERROR);

        /** Default singleton value of {@link ParsedEnum}. */
        public final static ParsedEnum DEFAULT = new ParsedEnum(Outcome.DEFAULT);

        /** Missing singleton value of {@link ParsedEnum}. */
        public final static ParsedEnum MISSING = new ParsedEnum(Outcome.MISSING);

        /** The outcome of the parsing. */
        private final Outcome outcome;

        /** The parsed value (when {@link Outcome} == {@link Outcome#PARSED}). */
        private final T value;

        /**
         * Creates a enum parsing result for the case when {@link Outcome} != {@link Outcome#PARSED}).
         * @param outcome The outcome of parsing.
         */
        private ParsedEnum(Outcome outcome) {
            this.outcome = outcome;
            this.value = null;
        }

        /**
         * Creates a enum parsing result for the case when a valid value has been parsed
         * ({@link Outcome} == {@link Outcome#PARSED}).
         * @param val The parsed value.
         */
        private ParsedEnum(T val) {
            this.outcome = Outcome.PARSED;
            this.value = val;
        }

        /**
         * Returns the outcome of the parsing.
         * @return the outcome of the parsing.
         */
        public Outcome outcome() {
            return outcome;
        }

        /**
         * Returns the parsed value (or asserts if this is not a valid {@link Outcome}).
         * @return The parsed value.
         */
        public T value() {
            assert outcome == Outcome.PARSED;
            return value;
        }

        /**
         * Returns whether the parameter was missing.
         * @return true if parameter was missing.
         */
        public boolean isMissing() {
            return outcome == Outcome.MISSING;
        }

        /**
         * Returns whether there was an error parsing the parameter.
         * @return true if there was an error parsing the parameter.
         */
        public boolean isError() {
            return outcome == Outcome.ERROR;
        }

        /**
         * Returns whether the {@link SqlKeyword#DEFAULT} value of parameter was specified.
         * @return true if the {@link SqlKeyword#DEFAULT} value of parameter was specified.
         */
        public boolean isDefault() {
            return outcome == Outcome.DEFAULT;
        }

        /**
         * Returns whether a valid value of parameter has been parsed.
         * @return true if a valid value of parameter has been parsed.
         */
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

    /**
     * Parses a boolean parameter. Recognizes several syntaxes.
     *
     * <p>Syntaxes:
     * <ul>
     *   <li>{@code trueKeyword} alone (e.g., "{@code LOGGING}"),
     *   <li>{@code falseKeyword} alone (e.g., "{@code NOLOGGING}"),
     *   <li>{@code trueKeyword=value} (e.g., {@code LOGGING=YES}, {@code LOGGING=FALSE}, {@code LOGGING=DEFAULT}).
     * </ul></p>
     *
     * @param lex The lexer.
     * @param trueKeyword Keyword for true value (if used alone) or {@code keyword=value} syntax.
     * @param falseKeyword Keyword for false value (shall be used alone).
     * @param parsedParams Parsed parameter list for duplicate checking. No duplicate checking performed
     *      if null is supplied.
     * @param allowDflt Allow {@code DEFAULT} keyword to be used instead of boolean value.
     * @param setter A closure/lambda to call to record the parsed value.
     */
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
     * Parses a boolean value allowing additional values defined in {@link BooleanEnum} and possible default value.
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
