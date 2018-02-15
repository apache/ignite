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
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvFormat;
import org.apache.ignite.internal.sql.SqlEscapeSeqParser;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.ignite.internal.sql.SqlEscapeSeqParser.State.PROCESSING;

/**
 * A {@link PipelineBlock}, which splits line according to CSV format rules and unquotes fields.
 * The next block {@link PipelineBlock#accept(Object, boolean)} is called per-line.
 */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {
    /** Field delimiter pattern. */
    @NotNull private final Pattern fldSeparator;

    /** Quote characters. */
    @NotNull private final String quoteChars;


    /** Line comment start characters. */
    @Nullable private final Pattern commentStartRe;

    /** Escape sequence start characters. */
    @Nullable private final String escapeChars;

    /** Parsing flags: see {@link BulkLoadCsvFormat} for details. */
    private final int flags;

    /** Leftover characters from previous field for multiline record case. */
    @NotNull private final StringBuilder processedField;

    /** FIXME SHQ */
    @NotNull private final List<String> fields;

    /** Current quote character. 0 if not in quotes. */
    private char quoteChr;

    /** FIXME SHQ */
    private final Pattern exactFieldSep;

    /** FIXME SHQ */
    private final Pattern fieldSepOrQuote;

    /** FIXME SHQ */
    private final Pattern quoteOrEscSeq;

    /**
     * Creates a CSV line parser.
     *
     * @param fldSeparator The pattern for the field delimiter.
     * @param quoteChars Quoting character.
     * @param commentStartRe Line comment start characters.
     * @param escapeChars Escape sequence start characters.
     * @param csvFlags Parsing flags from {@link BulkLoadCsvFormat}.
     */
    public CsvLineProcessorBlock(@NotNull Pattern fldSeparator, @NotNull String quoteChars,
        @Nullable Pattern commentStartRe, @Nullable String escapeChars, int csvFlags) {
        if ((csvFlags & BulkLoadCsvFormat.FLAG_TRIM_DELIM_WHITESPACE) != 0)
            fldSeparator = Pattern.compile("\\w*" + fldSeparator.pattern() + "\\w*");

        this.fldSeparator = fldSeparator;
        this.quoteChars = quoteChars;
        this.commentStartRe = commentStartRe;
        this.escapeChars = escapeChars;

        flags = csvFlags;

        exactFieldSep = Pattern.compile("^" + fldSeparator.pattern());

        // FIXME SHQ: convert quote chars to regex syntax
        fieldSepOrQuote = Pattern.compile(
            "(" + fldSeparator.pattern() + ")|([" + quoteChars + "])");

        // FIXME SHQ: convert escape chars to regex syntax
        quoteOrEscSeq = Pattern.compile(
            "([" + quoteChars + "])|([" + escapeChars + "])");

        fields = new LinkedList<>();
        processedField = new StringBuilder();
        quoteChr = 0;
    }

    /** {@inheritDoc} */
    @Override public void accept(String input, boolean isEof) throws IgniteCheckedException {
        input = stripComment(input);

        Matcher exactFieldSepMatcher = exactFieldSep.matcher(input);
        Matcher fieldSepOrQuoteMatcher = fieldSepOrQuote.matcher(input);
        Matcher quoteOrEscSeqMatcher = quoteOrEscSeq.matcher(input);

        int pos = 0;

        while (pos < input.length()) {
            if (quoteChr == 0) {
                if (!fieldSepOrQuoteMatcher.find(pos)) {
                    processedField.append(input, pos, input.length());

                    fields.add(processedField.toString());

                    processedField.setLength(0);

                    pos = input.length();

                    break;
                }

                int matchStart = fieldSepOrQuoteMatcher.start(1);
                if (matchStart != -1) { // Field separator found
                    processedField.append(input, pos, matchStart);

                    pos = fieldSepOrQuoteMatcher.end(1);

                    fields.add(processedField.toString());

                    processedField.setLength(0);

                    continue;
                }

                // Quote found
                matchStart = fieldSepOrQuoteMatcher.start(2);
                assert matchStart >= 0 && matchStart < input.length();
                if (matchStart == pos) {
                    pos++;

                    quoteChr = input.charAt(matchStart);
                    state = State.IN_QUOTED_FIELD;
                    assert processedField.length() == 0;
                    continue;
                }

                if ((flags & BulkLoadCsvFormat.FLAG_STRICT_QUOTES) != 0) {
                    IgniteBiTuple<Integer, String> handlerRes =
                        handleError(input, matchStart, "Quote occurs in the middle of the field");

                    assert handlerRes.get1() > pos;
                    pos = handlerRes.get1();
                    processedField.append(handlerRes.get2());

                    continue;
                }
                else {
                    int endPos = fieldSepOrQuoteMatcher.end(2);
                    processedField.append(input, pos, endPos);
                    pos = endPos;
                    continue;
                }
            }
            else if (state == State.IN_QUOTED_FIELD) {
                assert quoteChr != 0;

                //while (pos < input.length()) {
                if (!quoteOrEscSeqMatcher.find(pos)) {
                    processedField.append(input, pos, input.length());

                    pos = input.length();

                    break;
                }

                int foundPos = quoteOrEscSeqMatcher.start(1);
                if (foundPos != -1) { // Quote found
                    assert foundPos >= 0 && foundPos < input.length();

                    if (input.charAt(foundPos) != quoteChr) {
                        processedField.append(input, pos, foundPos + 1);
                        pos = foundPos + 1;
                        continue;
                    }

                    if (foundPos < input.length() - 1 && input.charAt(foundPos + 1) == quoteChr) {
                        processedField.append(input, pos, foundPos + 1);
                        pos = foundPos + 2;
                        continue;
                    }

                    if (foundPos < input.length() - 1) {
                        exactFieldSepMatcher.region(foundPos + 1, input.length());

                        if (!exactFieldSepMatcher.matches()) {
                            if ((flags & BulkLoadCsvFormat.FLAG_STRICT_QUOTES) != 0) {
                                IgniteBiTuple<Integer, String> handlerRes =
                                    handleError(input, foundPos, "Extra characters after ending quote");

                                assert handlerRes.get1() > pos;
                                pos = handlerRes.get1();

                                processedField.append(handlerRes.get2());

                                continue;
                            }
                            else {
                                processedField.append(input, pos, foundPos + 1);
                                pos = foundPos + 1;
                                continue;
                            }
                        }

                        processedField.append(input, pos, foundPos);

                        pos = exactFieldSepMatcher.end();

                        fields.add(processedField.toString());

                        processedField.setLength(0);

                        state = State.AT_FIELD_START;
                        quoteChr = 0;

                        continue;
                    }
                    else {
                        processedField.append(input, pos, input.length());

                        pos = input.length();

                        fields.add(processedField.toString());

                        processedField.setLength(0);

                        state = State.AT_FIELD_START;
                        quoteChr = 0;

                        break;
                    }
                }
                else { // Escape sequence start found
                    foundPos = quoteOrEscSeqMatcher.start(2);
                    assert foundPos >= 0 && foundPos < input.length();

                    processedField.append(input, pos, foundPos);

                    pos = foundPos;

                    SqlEscapeSeqParser escParser = new SqlEscapeSeqParser();

                    SqlEscapeSeqParser.State r = PROCESSING;

                    do {
                        pos++;
                        r = pos < input.length() ? escParser.accept(input.charAt(pos)) : escParser.acceptEnd();
                    }
                    while (r == PROCESSING);

                    switch (r) {
                        case FINISHED_CHAR_REJECTED:
                            break;

                        case FINISHED_CHAR_ACCEPTED:
                            pos++;
                            break;

                        case ERROR:
                            // errors are not reported for now
                            // just ignore invalid escape sequenceы
                            break;

                        default:
                            throw new IgniteIllegalStateException("Unknown escape sequence parser state: " + r);
                    }

                    processedField.append(escParser.convertedStr());
                }

                // copy remainder of string?
            }
        }

        // remainder?

        assert nextBlock != null;

        nextBlock.accept(flds.toArray(new String[flds.size()]), isEof);
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
     * @param input The input line.
     * @return The line with the comment stripped.
     */
    @Nullable private String stripComment(@NotNull String input) {
        if (commentStartRe == null)
            return input;

        Matcher commentMatcher = commentStartRe.matcher(input);

        if (commentMatcher.find())
            return (commentMatcher.start() == 0) ? "" : input.substring(0, commentMatcher.start());
        else
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

    /** FIXME SHQ */
    @Override protected IgniteBiTuple<Integer, String> handleError(String input, int pos, String errorMsg) {
        return new IgniteBiTuple<>(pos + 1, input.substring(pos, pos + 1));
    }
}
