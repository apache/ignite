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
import java.util.regex.Pattern;

import static org.apache.ignite.internal.processors.bulkload.BulkLoadParameters.DEFAULT_INPUT_CHARSET;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_CONTINUE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_EOF;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_ERROR;

/** CSV parser for COPY command. */
public class BulkLoadCsvParser extends BulkLoadParser {

    /** Next batch index (for a very simple check that all batches were delivered to us). */
    private long nextBatchIdx;

    /** Processing pipeline input block: a decoder for the input stream of bytes */
    private final PipelineBlock<byte[], char[]> inputBlock;

    /** A record collecting block that appends its input to {@code List<String>}. */
    private final StrListAppenderBlock collectorBlock;

    /**
     * Creates bulk load CSV parser.

     *  @param format Format options (parsed from COPY command on the server side).
     */
    public BulkLoadCsvParser(BulkLoadCsvFormat fmt) {
        nextBatchIdx = 0;

        inputBlock = new CharsetDecoderBlock(DEFAULT_INPUT_CHARSET);

        collectorBlock = new StrListAppenderBlock();

        if (fmt.lineSeparatorRe() == null)
            fmt.lineSeparatorRe(BulkLoadCsvFormat.DEFAULT_LINE_SEP_RE);

        if (fmt.fieldSeparatorRe() == null)
            fmt.fieldSeparatorRe(BulkLoadCsvFormat.DEFAULT_FIELD_SEP_RE);

        if (fmt.commentChars() == null)
            fmt.commentChars(BulkLoadCsvFormat.DEFAULT_COMMENT_CHARS);

        if (fmt.quoteChars() == null)
            fmt.quoteChars(BulkLoadCsvFormat.DEFAULT_QUOTE_CHARS);

        if (fmt.escapeChars() == null)
            fmt.escapeChars(BulkLoadCsvFormat.DEFAULT_ESCAPE_CHARS);

        inputBlock.append(new LineSplitterBlock(fmt.lineSeparatorRe()))
                  .append(new CsvLineProcessorBlock(fmt.fieldSeparatorRe(), fmt.quoteChars(),
                      fmt.commentChars(), fmt.escapeChars()))
                  .append(collectorBlock);
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

        collectorBlock.output(result);

        inputBlock.accept(req.data(), isLastBatch);

        return result;
    }
}
