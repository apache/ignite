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

/** A placeholder for bulk load CSV format parser options. */
public class BulkLoadCsvFormat extends BulkLoadFormat {

    /** Line separator pattern. */
    public static final Pattern DEFAULT_LINE_SEP_RE = Pattern.compile("[\r\n]+");

    /** Field separator pattern. */
    public static final Pattern DEFAULT_FIELD_SEP_RE = Pattern.compile(",");

    /** Quote characters */
    public static final String DEFAULT_QUOTE_CHARS = "\"";

    /** Default escape sequence start characters. */
    public static final String DEFAULT_ESCAPE_CHARS = "\\";

    /** Line comment start pattern. */
    public static final Pattern DEFAULT_COMMENT_CHARS = null;

    /** Format name. */
    public static final String NAME = "CSV";

    private Pattern lineSeparatorRe;
    private Pattern fieldSeparatorRe;
    private String quoteChars;
    private Pattern commentChars;
    private String escapeChars;

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
    public Pattern lineSeparatorRe() {
        return lineSeparatorRe;
    }

    /**
     * Sets the line separator pattern.
     *
     * @param lineSeparatorRe The line separator pattern.
     */
    public void lineSeparatorRe(Pattern lineSeparatorRe) {
        this.lineSeparatorRe = lineSeparatorRe;
    }

    /**
     * Returns the field separator pattern.
     *
     * @return The field separator pattern.
     */
    public Pattern fieldSeparatorRe() {
        return fieldSeparatorRe;
    }

    /**
     * Sets the field separator pattern.
     *
     * @param fieldSeparatorRe The field separator pattern.
     */
    public void fieldSeparatorRe(Pattern fieldSeparatorRe) {
        this.fieldSeparatorRe = fieldSeparatorRe;
    }

    /**
     * Returns the quote characters.
     *
     * @return The quote characters.
     */
    public String quoteChars() {
        return quoteChars;
    }

    /**
     * Sets the quote characters.
     *
     * @param quoteChars The quote characters.
     */
    public void quoteChars(String quoteChars) {
        this.quoteChars = quoteChars;
    }

    /**
     * Returns the line comment start pattern.
     *
     * @return The line comment start pattern.
     */
    public Pattern commentChars() {
        return commentChars;
    }

    /**
     * Sets the line comment start pattern.
     *
     * @param commentChars The line comment start pattern.
     */
    public void commentChars(Pattern commentChars) {
        this.commentChars = commentChars;
    }

    /**
     * Returns the escape characters.
     *
     * @return The escape characters.
     */
    public String escapeChars() {
        return escapeChars;
    }

    /**
     * Sets the escape characters.
     *
     * @param escapeChars The escape characters.
     */
    public void escapeChars(String escapeChars) {
        this.escapeChars = escapeChars;
    }
}
