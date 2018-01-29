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
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadContext;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.apache.ignite.internal.processors.bulkload.BulkLoadParameters.DEFAULT_INPUT_CHARSET;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_CONTINUE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_EOF;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_ERROR;

/** CSV parser for COPY command. */
public class BulkLoadCsvParser extends BulkLoadParser {

    /** Next batch index (for a very simple check that all batches were delivered to us). */
    private long nextBatchIdx;

    /** Decoder for the input stream of bytes */
    private final PipelineBlock<byte[], char[]> decoder;

    /** A block that appends its input to {@code List<String>}. Used as a records collector. */
    private final StrListAppenderBlock strListAppenderBlock;

    /**
     * Creates bulk load CSV parser.
     *
     * @param format Format options (parsed from COPY command on the server side).
     * @param params Input file parameters.
     */
    public BulkLoadCsvParser(BulkLoadFormat format, BulkLoadParameters params) {
        super(format);

        nextBatchIdx = 0;

        decoder = new CharsetDecoderBlock(DEFAULT_INPUT_CHARSET);

        strListAppenderBlock = new StrListAppenderBlock();

        decoder.append(new LineSplitterBlock(BulkLoadCsvFormat.LINE_SEP_RE))
               .append(new CsvLineProcessorBlock(BulkLoadCsvFormat.FIELD_SEP_RE, BulkLoadCsvFormat.QUOTE_CHARS))
               .append(strListAppenderBlock);
    }

    /** {@inheritDoc} */
    @Override public Iterable<List<Object>> processBatch(JdbcBulkLoadContext ctx, JdbcBulkLoadBatchRequest req)
        throws IgniteCheckedException {

        if (nextBatchIdx != req.batchIdx())
            throw new IgniteSQLException("Batch #" + (nextBatchIdx + 1) +
                " is missing. Received #" + req.batchIdx() + " instead.");

        nextBatchIdx++;

        switch (req.cmd()) {
            case CMD_FINISHED_EOF:
                return parseBatch(req, true);

            case CMD_CONTINUE:
                return parseBatch(req, false);

            case CMD_FINISHED_ERROR:
                return Collections.emptyList();

            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Parses a batch of records
     * @param req The request with all parameters.
     * @param isLastBatch true, if it is a last batch.
     * @return Iterable over the parsed records.
     * @throws IgniteCheckedException if parsing has failed.
     */
    private Iterable<List<Object>> parseBatch(JdbcBulkLoadBatchRequest req, boolean isLastBatch)
        throws IgniteCheckedException {

        List<List<Object>> result = new LinkedList<>();
        strListAppenderBlock.output(result);

        decoder.accept(req.data(), isLastBatch);

        return result;
    }
}
