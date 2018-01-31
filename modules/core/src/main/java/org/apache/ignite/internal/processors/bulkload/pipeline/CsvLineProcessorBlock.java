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

package org.apache.ignite.internal.processors.bulkload.pipeline;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.sql.SqlEscapeSeqParser;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link PipelineBlock}, which splits line according to CSV format rules and unquotes fields.
 *
 * <p>The next block {@link PipelineBlock#accept(Object, boolean)} is called per-line.
 */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {

    /** Field delimiter pattern. */
    private final Pattern fieldDelimiter;

    /* Quote characters. */
    private final String quoteChars;

    /** Line comment start characters. */
    @Nullable private final Pattern commentStartRe;

    /** Escape sequence start characters. */
    @Nullable private final String escapeChars;

    /**
     * Creates a CSV line parser.
     *
     * @param fieldDelimiter The pattern for the field delimiter.
     * @param quoteChars Quoting character.
     * @param commentStartRe Line comment start characters.
     * @param escapeChars Escape sequence start characters.
     */
    public CsvLineProcessorBlock(@NotNull Pattern fieldDelimiter, @NotNull String quoteChars,
        @Nullable Pattern commentStartRe, @Nullable String escapeChars) {

        super();

        this.commentStartRe = commentStartRe;
        this.escapeChars = escapeChars;
        this.fieldDelimiter = fieldDelimiter;
        this.quoteChars = quoteChars;
    }

    /** {@inheritDoc} */
    @Override public void accept(String input, boolean isEof) throws IgniteCheckedException {

        input = stripComment(input);

        if (F.isEmpty(input))
            return;

        String[] output = fieldDelimiter.split(input);

        for (int i = 0; i < output.length; i++)
            output[i] = replaceEscSeq(trim(output[i]));

        nextBlock.accept(output, isEof);
    }

    /**
     * Trims quote characters from beginning and end of the string.
     * If ony one character is specified, the quotes are not stripped (without reporting an error).
     *
     * @param str String to trim.
     * @return The trimmed string.
     */
    @NotNull private String trim(String str) {
        if (quoteChars.indexOf(str.charAt(0)) == -1)
            return str;

        if (quoteChars.indexOf(str.charAt(str.length() - 1)) == -1)
            return str;

        return str.substring(1, str.length() - 1);
    }

    /**
     * Strips the line comment, if exists.
     *
     * @param input Input line
     * @return The line with comment stripped or null if the comment occupied the whole line
     */
    @Nullable private String stripComment(@NotNull String input) {
        if (commentStartRe != null) {
            Matcher commentMatcher = commentStartRe.matcher(input);

            if (commentMatcher.find()) {
                if (commentMatcher.start() == 0)
                    return null;

                return input.substring(0, commentMatcher.start());
            }
        }

        return input;
    }

    /**
     * Replaces escape sequences in the string. Invalid escape sequences are silently removed.
     *
     * @param input The string to process.
     * @return The result with escape sequences replaced and invalid escape sequences removed.
     */
    @Nullable private String replaceEscSeq(@Nullable String input) {
        if (escapeChars == null)
            return input;

        return SqlEscapeSeqParser.replaceAll(input, escapeChars, null);
    }
}
