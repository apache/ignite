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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.bulkload.pipeline.CharsetDecoderBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.CsvLineProcessorBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.PipelineBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.StrListAppenderBlock;
import org.apache.ignite.internal.processors.bulkload.pipeline.LineSplitterBlock;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest;

import java.util.LinkedList;
import java.util.List;

/** CSV parser for COPY command. */
public class BulkLoadCsvParser extends BulkLoadParser {
    /** Processing pipeline input block: a decoder for the input stream of bytes */
    private final PipelineBlock<byte[], char[]> inputBlock;

    /** A record collecting block that appends its input to {@code List<String>}. */
    private final StrListAppenderBlock collectorBlock;

    /**
     * Creates bulk load CSV parser.
     *
     *  @param format Format options (parsed from COPY command on the server side).
     */
    public BulkLoadCsvParser(BulkLoadCsvFormat format) {
        inputBlock = new CharsetDecoderBlock(BulkLoadFormat.DEFAULT_INPUT_CHARSET);

        collectorBlock = new StrListAppenderBlock();

        // Handling of the other options is to be implemented in IGNITE-7537.
        inputBlock.append(new LineSplitterBlock(format.lineSeparatorRe()))
               .append(new CsvLineProcessorBlock(format.fieldSeparatorRe(), format.quoteChars()))
               .append(collectorBlock);
    }

    /**
     * Parses a batch of records.
     *
     * @param req The request with all parameters.
     * @param isLastBatch true, if it is a last batch.
     * @return Iterable over the parsed records.
     * @throws IgniteCheckedException if parsing has failed.
     */
    @Override protected Iterable<List<Object>> parseBatch(JdbcBulkLoadBatchRequest req, boolean isLastBatch)
        throws IgniteCheckedException {
        List<List<Object>> res = new LinkedList<>();

        collectorBlock.output(res);

        inputBlock.accept(req.data(), isLastBatch);

        return res;
    }
}
