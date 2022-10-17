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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvFormat;

/**
 * A {@link PipelineBlock}, which splits line according to CSV format rules and unquotes fields. The next block {@link
 * PipelineBlock#accept(Object, boolean)} is called per-line.
 */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {
    /** Empty string array. */
    public static final String[] EMPTY_STR_ARRAY = new String[0];

    /** Field delimiter pattern. */
    private final char fldDelim;

    /** Quote character. */
    private final char quoteChars;

    /** Null string. */
    private final String nullString;

    /** Trim field string content. */
    private final boolean trim;

    /** Lines count. */
    private int line = 0;

    /** Symbol count. */
    private int symbol = 0;

    /**
     * Creates a CSV line parser.
     */
    public CsvLineProcessorBlock(BulkLoadCsvFormat format) {
        this.fldDelim = format.fieldSeparator().toString().charAt(0);
        this.quoteChars = format.quoteChars().charAt(0);
        this.nullString = format.nullString();
        this.trim = format.trim();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void accept(String input, boolean isLastPortion) throws IgniteCheckedException {
        List<String> fields = new ArrayList<>();

        StringBuilder currentField = new StringBuilder(256);

        ReaderState state = ReaderState.IDLE;

        final int length = input.length();
        int copy = 0;
        int current = 0;
        int prev = -1;
        int copyStart = 0;

        boolean quotesMatched = true;

        line++;
        symbol = 0;

        while (true) {
            if (current == length) {
                if (!quotesMatched)
                    throw new IgniteCheckedException(new SQLException("Unmatched quote found at the end of line "
                        + line + ", symbol " + symbol));

                if (copy > 0)
                    currentField.append(input, copyStart, copyStart + copy);

                addField(fields, currentField, prev == quoteChars);

                break;
            }

            final char c = input.charAt(current++);
            symbol++;

            if (state == ReaderState.QUOTED) {
                if (c == quoteChars) {
                    state = ReaderState.IDLE;
                    quotesMatched = !quotesMatched;

                    if (copy > 0) {
                        currentField.append(input, copyStart, copyStart + copy);

                        copy = 0;
                    }

                    copyStart = current;
                }
                else
                    copy++;
            }
            else {
                if (c == fldDelim) {
                    if (copy > 0) {
                        currentField.append(input, copyStart, copyStart + copy);

                        copy = 0;
                    }

                    addField(fields, currentField, prev == quoteChars);

                    currentField = new StringBuilder();
                    copyStart = current;

                    state = ReaderState.IDLE;
                }
                else if (c == quoteChars && state != ReaderState.UNQUOTED) {
                    state = ReaderState.QUOTED;

                    quotesMatched = !quotesMatched;

                    if (prev == quoteChars)
                        copy++;
                    else
                        copyStart = current;
                }
                else {
                    if (c == quoteChars) {
                        if (state == ReaderState.UNQUOTED)
                            throw new IgniteCheckedException(
                                new SQLException("Unexpected quote in the field, line " + line
                                    + ", symbol " + symbol));

                        quotesMatched = !quotesMatched;
                    }

                    copy++;

                    if (state == ReaderState.IDLE)
                        state = ReaderState.UNQUOTED;
                }
            }

            prev = c;
        }

        nextBlock.accept(fields.toArray(EMPTY_STR_ARRAY), isLastPortion);
    }

    /**
     *
     * @param fields row fields.
     * @param fieldVal field value.
     */
    private void addField(List<String> fields, StringBuilder fieldVal, boolean quoted) {
        final String val = trim ? fieldVal.toString().trim() : fieldVal.toString();

        fields.add(val.equals(nullString) ? null : val);
    }

    /**
     *
     */
    private enum ReaderState {
        /** */
        IDLE,

        /** */
        UNQUOTED,

        /** */
        QUOTED
    }
}
