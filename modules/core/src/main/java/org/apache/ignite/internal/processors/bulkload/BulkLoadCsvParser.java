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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.bulkload.pipeline.CharsetDecoderBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.CsvLineProcessorBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.PipelineBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.StrListAppenderBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.LineSplitterBlock;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

/** CSV parser for COPY command. */
public class BulkLoadCsvParser extends BulkLoadParser {
    /** Processing pipeline input block: a decoder for the input stream of bytes */
    private final PipelineBlock<byte[], char[]> inputBlock;

    /** A record collecting block that appends its input to {@code List<String>}. */
    private final StrListAppenderBlock collectorBlock;

    /**
     * Creates bulk load CSV parser.
     *
     *  @param format Format options (parsed from COPY command).
     */
    public BulkLoadCsvParser(BulkLoadCsvFormat format) {
        try {
            Charset charset = format.inputCharsetName() == null ? BulkLoadFormat.DEFAULT_INPUT_CHARSET :
                Charset.forName(format.inputCharsetName());

            inputBlock = new CharsetDecoderBlock(charset);
        }
        catch (IllegalCharsetNameException e) {
            throw new IgniteSQLException("Unknown charset name: '" + format.inputCharsetName() + "': " +
                e.getMessage());
        }
        catch (UnsupportedCharsetException e) {
            throw new IgniteSQLException("Charset is not supported: '" + format.inputCharsetName() + "': " +
                e.getMessage());
        }

        collectorBlock = new StrListAppenderBlock();

        // Handling of the other options is to be implemented in IGNITE-7537.
        inputBlock.append(new LineSplitterBlock(format.lineSeparator()))
               .append(new CsvLineProcessorBlock(format.fieldSeparator(), format.quoteChars()))
               .append(collectorBlock);
    }

    /** {@inheritDoc} */
    @Override protected Iterable<List<Object>> parseBatch(byte[] batchData, boolean isLastBatch)
        throws IgniteCheckedException {
        List<List<Object>> res = new LinkedList<>();

        collectorBlock.output(res);

        inputBlock.accept(batchData, isLastBatch);

        return res;
    }
}
