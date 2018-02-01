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
import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

/**
 * A {@link PipelineBlock}, which splits line according to CSV format rules and unquotes fields.
 * The next block {@link PipelineBlock#accept(Object, boolean)} is called per-line.
 */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {
    /** Field delimiter pattern. */
    private final Pattern fieldDelimiter;

    /* Quote character. */
    private final String quoteChars;

    /**
     * Creates a CSV line parser.
     *
     * @param fieldDelimiter The pattern for the field delimiter.
     * @param quoteChars Quoting character.
     */
    public CsvLineProcessorBlock(Pattern fieldDelimiter, String quoteChars) {
        this.fieldDelimiter = fieldDelimiter;
        this.quoteChars = quoteChars;
    }

    /** {@inheritDoc} */
    @Override public void accept(String input, boolean isLastPortion) throws IgniteCheckedException {
        String[] fields = fieldDelimiter.split(input);

        for (int i = 0; i < fields.length; i++)
            fields[i] = trim(fields[i]);

        nextBlock.accept(fields, isLastPortion);
    }

    /**
     * Trims quote characters from beginning and end of the line.
     *
     * @param str String to trim.
     * @return The trimmed string.
     */
    @NotNull private String trim(String str) {
        int startPos = quoteChars.indexOf(str.charAt(0)) != -1 ? 1 : 0;
        int endPos = quoteChars.indexOf(str.charAt(str.length() - 1)) != -1 ? str.length() - 1 : str.length();

        return str.substring(startPos, endPos);
    }
}
