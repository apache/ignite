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

package org.apache.ignite.internal.processors.bulkload;

import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A placeholder for bulk load CSV format parser options.
 */
public class BulkLoadCsvFormat extends BulkLoadFormat {
    /** Line separator pattern. */
    @NotNull public static final Pattern DEFAULT_LINE_SEPARATOR = Pattern.compile("[\r\n]+");

    /** Field separator pattern. */
    @NotNull public static final Pattern DEFAULT_FIELD_SEPARATOR = Pattern.compile(",");

    /** Quote characters */
    @NotNull public static final String DEFAULT_QUOTE_CHARS = "\"";

    /** Default escape sequence start characters. */
    @Nullable public static final String DEFAULT_ESCAPE_CHARS = null;

    /** Line comment start pattern. */
    @Nullable public static final Pattern DEFAULT_COMMENT_CHARS = null;

    /** Format name. */
    public static final String NAME = "CSV";

    /** Line separator pattern. */
    @Nullable private Pattern lineSeparator;

    /** Field separator pattern. */
    @Nullable private Pattern fieldSeparator;

    /** Set of quote characters. */
    @Nullable private String quoteChars;

    /** Line comment start pattern. */
    @Nullable private Pattern commentChars;

    /** Set of escape start characters. */
    @Nullable private String escapeChars;

    /** File charset. */
    @Nullable private String inputCharsetName;

    /**
     * Returns the name of the format.
     *
     * @return The name of the format.
     */
    @Override public String name() {
        return NAME;
    }

    /**
     * Returns the line separator pattern.
     *
     * @return The line separator pattern.
     */
    @Nullable public Pattern lineSeparator() {
        return lineSeparator;
    }

    /**
     * Sets the line separator pattern.
     *
     * @param lineSeparator The line separator pattern.
     */
    public void lineSeparator(@Nullable Pattern lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    /**
     * Returns the field separator pattern.
     *
     * @return The field separator pattern.
     */
    @Nullable public Pattern fieldSeparator() {
        return fieldSeparator;
    }

    /**
     * Sets the field separator pattern.
     *
     * @param fieldSeparator The field separator pattern.
     */
    public void fieldSeparator(@Nullable Pattern fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
    }

    /**
     * Returns the quote characters.
     *
     * @return The quote characters.
     */
    @Nullable public String quoteChars() {
        return quoteChars;
    }

    /**
     * Sets the quote characters.
     *
     * @param quoteChars The quote characters.
     */
    public void quoteChars(@Nullable String quoteChars) {
        this.quoteChars = quoteChars;
    }

    /**
     * Returns the line comment start pattern.
     *
     * @return The line comment start pattern.
     */
    @Nullable public Pattern commentChars() {
        return commentChars;
    }

    /**
     * Sets the line comment start pattern.
     *
     * @param commentChars The line comment start pattern.
     */
    public void commentChars(@Nullable Pattern commentChars) {
        this.commentChars = commentChars;
    }

    /**
     * Returns the escape characters.
     *
     * @return The escape characters.
     */
    @Nullable public String escapeChars() {
        return escapeChars;
    }

    /**
     * Sets the escape characters.
     *
     * @param escapeChars The escape characters.
     */
    public void escapeChars(@Nullable String escapeChars) {
        this.escapeChars = escapeChars;
    }

    /**
     * Returns the input file charset name, null if not specified.
     *
     * @return The input file charset name, null if not specified.
     */
    @Nullable public String inputCharsetName() {
        return inputCharsetName;
    }

    /**
     * Sets the input file charset name. The null here means "not specified".
     *
     * @param inputCharsetName The input file charset name.
     */
    public void inputCharsetName(@Nullable String inputCharsetName) {
        this.inputCharsetName = inputCharsetName;
    }
}
