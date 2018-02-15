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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.jetbrains.annotations.NotNull;

/**
 * Speed-optimized CSV file parser, which processes both lines and fields. Unifinished fields and lines
 * are kept between invocations of {@link #accept(char[], boolean)}.
 * <p>
 * Please note that speed of parsing was of higher priority than using "proper" OOP design patterns.
 * Regular expressions aren't used here for the same reason.
 */
public class CsvParserBlock extends PipelineBlock<char[], List<Object>> {
    /** Leftover characters from the previous invocation of {@link #accept(char[], boolean)}. */
    private final StringBuilder leftover;

    private int leftoverQuotesCnt;
    private int leftoverEscapesCnt;

    /** Current parsed fields from the beginning of the line. */
    private final List<Object> fields;

    private final static byte CLS_NONE = 0;
    private final static byte CLS_LINESEP = 1;
    private final static byte CLS_FIELDSEP = 2;
    private final static byte CLS_QUOTE = 3;
    private final static byte CLS_ESCAPE = 4;
    private final static byte CLS_COMMENT = 5;

    private final byte[] charClass;

    /**
     * Creates line splitter block.
     */
    public CsvParserBlock() {
        leftover = new StringBuilder();
        fields = new ArrayList<>();

        charClass = new byte[Character.MAX_VALUE];

        defineCharClass("\n\r", CLS_LINESEP);
        defineCharClass(",", CLS_FIELDSEP);
        defineCharClass("\"", CLS_QUOTE);

        leftoverQuotesCnt = 0;
        leftoverEscapesCnt = 0;
    }

    private void defineCharClass(String chars, byte newCls) {
        for (int i = 0; i < chars.length(); i++) {
            char c = chars.charAt(i);
            byte existingCls = charClass[c];

            if (existingCls != CLS_NONE && existingCls != newCls)
                throw new IgniteSQLException("The same character used in two or more separators: '" + c + "'");

            charClass[c] = newCls;
        }
    }

    /** {@inheritDoc} */
    @Override public void accept(char[] chars, boolean isLastPortion) throws IgniteCheckedException {
        leftover.append(chars);

        int lastPos = 0;

        for (int i = 0; i < leftover.length(); i++) {
            char curChr = leftover.charAt(i);

            switch (charClass[curChr]) {
                case CLS_FIELDSEP:
                    addLeftoverSubstrToFields(lastPos, i);

                    lastPos = i + 1;

                    break;

                case CLS_LINESEP:
                    addLeftoverSubstrToFields(lastPos, i);

                    pushFieldsToNextBlock(false);

                    lastPos = i + 1;

                    if (lastPos < leftover.length()) {
                        char nextChr = leftover.charAt(lastPos);
                        if (charClass[nextChr] == CLS_LINESEP && nextChr != curChr) {
                            lastPos++;
                            i++;
                        }
                    }

                    break;

                case CLS_QUOTE:
                    leftoverQuotesCnt++;

                    break;

                case CLS_ESCAPE:
                    leftoverEscapesCnt++;

                    break;
            }
        }

        if (lastPos >= leftover.length())
            leftover.setLength(0);
        else if (lastPos != 0)
            leftover.delete(0, lastPos);

        if (isLastPortion && leftover.length() > 0) {
            addLeftoverSubstrToFields(0, leftover.length());

            leftover.setLength(0);

            pushFieldsToNextBlock(true);
        }
    }

    private void addLeftoverSubstrToFields(int lastPos, int i) {
        leftoverQuotesCnt = 0;
        leftoverEscapesCnt = 0;

        fields.add(unquotedSubstring(leftover, lastPos, i));
    }

    private void pushFieldsToNextBlock(boolean isLastBlock) throws IgniteCheckedException {
        nextBlock.accept(new ArrayList<>(fields), isLastBlock);

        fields.clear();
    }

    /**
     * Extracts substring from the {@code str}, omitting quotes if they are found exactly at substring boundaries.
     *
     * @param str The string to take substring from.
     * @param from The beginning index.
     * @param to The end index (last character position + 1).
     * @return The substring without quotes.
     */
    @NotNull private String unquotedSubstring(@NotNull StringBuilder str, int from, int to) {
        if ((to - from) >= 2) {
            char fromChr = str.charAt(from);

            if (charClass[fromChr] == CLS_QUOTE && charClass[str.charAt(to - 1)] == fromChr)
                return str.substring(from + 1, to - 1);
        }

        return str.substring(from, to);
    }
}
