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

    /** Current parsed fields from the beginning of the line. */
    private final List<Object> fields;

    /**
     * Creates line splitter block.
     */
    public CsvParserBlock() {
        leftover = new StringBuilder();
        fields = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public void accept(char[] chars, boolean isLastPortion) throws IgniteCheckedException {
        leftover.append(chars);

        int lastPos = 0;

        for (int i = 0; i < leftover.length(); i++) {
            switch (leftover.charAt(i)) {
                case ',':
                    fields.add(unquotedSubstring(leftover, lastPos, i));

                    lastPos = i + 1;

                    break;

                case '\r':
                case '\n':
                    fields.add(unquotedSubstring(leftover, lastPos, i));

                    nextBlock.accept(new ArrayList<>(fields), false);

                    fields.clear();

                    lastPos = i + 1;

                    if (lastPos < leftover.length() && leftover.charAt(lastPos) == '\n') {
                        lastPos++;
                        i++;
                    }

                    break;
            }
        }

        if (lastPos >= leftover.length())
            leftover.setLength(0);
        else if (lastPos != 0)
            leftover.delete(0, lastPos);

        if (isLastPortion && leftover.length() > 0) {
            fields.add(unquotedSubstring(leftover, 0, leftover.length()));

            leftover.setLength(0);

            nextBlock.accept(new ArrayList<>(fields), true);

            fields.clear();
        }
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
        if ((to - from) >= 2 && str.charAt(from) == '"' && str.charAt(to - 1) == '"')
            return str.substring(from + 1, to - 1);

        return str.substring(from, to);
    }
}
