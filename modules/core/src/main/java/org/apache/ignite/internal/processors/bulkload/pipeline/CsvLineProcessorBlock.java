/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.bulkload.pipeline;

import org.apache.ignite.IgniteCheckedException;

import java.util.regex.Pattern;

/**
 * A {@link PipelineBlock}, which splits line according to CSV format rules and unquotes fields.
 * The next block {@link PipelineBlock#accept(Object, boolean)} is called per-line.
 */
public class CsvLineProcessorBlock extends PipelineBlock<String, String[]> {
    /** Field delimiter pattern. */
    private final Pattern fldDelim;

    /** Quote character. */
    private final String quoteChars;

    /**
     * Creates a CSV line parser.
     *
     * @param fldDelim The pattern for the field delimiter.
     * @param quoteChars Quoting character.
     */
    public CsvLineProcessorBlock(Pattern fldDelim, String quoteChars) {
        this.fldDelim = fldDelim;
        this.quoteChars = quoteChars;
    }

    /** {@inheritDoc} */
    @Override public void accept(String input, boolean isLastPortion) throws IgniteCheckedException {
        // Currently we don't process quoted field delimiter properly, will be fixed in IGNITE-7537.
        String[] fields = fldDelim.split(input);

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
    private String trim(String str) {
        if (str.isEmpty())
            return null;

        int startPos = quoteChars.indexOf(str.charAt(0)) != -1 ? 1 : 0;
        int endPos = quoteChars.indexOf(str.charAt(str.length() - 1)) != -1 ? str.length() - 1 : str.length();

        return str.substring(startPos, endPos);
    }
}
