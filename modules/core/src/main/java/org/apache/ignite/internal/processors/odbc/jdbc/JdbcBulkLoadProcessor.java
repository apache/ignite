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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_CONTINUE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_EOF;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_ERROR;

/**
 * JDBC wrapper around {@link BulkLoadProcessor} that provides extra logic.
 *
 * Unlike other "single shot" request-reply commands, the
 * COPY command the client-server interaction looks like this:
 *
 * <pre>
 * Thin JDBC client                            Server
 *        |                                       |
 *        |------- JdbcQueryExecuteRequest ------>|
 *        |         with SQL copy command         |
 *        |                                       |
 *        |<---- JdbcBulkLoadAckResult -----------|
 *        | with BulkLoadAckClientParameters      |
 *        | containing file name and batch size.  |
 *        |                                       |
 * (open the file,                                |
 *  read portions and send them)                  |
 *        |                                       |
 *        |------- JdbcBulkLoadBatchRequest #1 -->|
 *        | with a portion of input file.         |
 *        |                                       |
 *        |<--- JdbcQueryExecuteResult -----------|
 *        | with current update counter.          |
 *        |                                       |
 *        |------- JdbcBulkLoadBatchRequest #2--->|
 *        | with a portion of input file.         |
 *        |                                       |
 *        |<--- JdbcQueryExecuteResult -----------|
 *        | with current update counter.          |
 *        |                                       |
 *        |------- JdbcBulkLoadBatchRequest #3--->|
 *        | with the LAST portion of input file.  |
 *        |                                       |
 *        |<--- JdbcQueryExecuteResult -----------|
 *        | with the final update counter.        |
 *        |                                       |
 * (close the file)                               |
 *        |                                       |
 * </pre>
 *
 * In case of input file reading error, a flag is carried to the server:
 * {@link JdbcBulkLoadBatchRequest#CMD_FINISHED_ERROR} and the processing
 * is aborted on the both sides.
 */
public class JdbcBulkLoadProcessor extends JdbcCursor {
    /** A core processor that handles incoming data packets. */
    private final BulkLoadProcessor processor;

    /** Next batch index (for a very simple check that all batches were delivered to us). */
    protected long nextBatchIdx;

    /**
     * Creates a JDBC-specific adapter for bulk load processor.
     *
     * @param processor Bulk load processor from the core to delegate calls to.
     * @param reqId Id of the request that created given processor.
     */
    public JdbcBulkLoadProcessor(BulkLoadProcessor processor, long reqId) {
        super(reqId);

        this.processor = processor;
        nextBatchIdx = 0;
    }

    /**
     * Completely processes a bulk load batch request.
     *
     * Calls {@link BulkLoadProcessor} wrapping around some JDBC-specific logic
     * (commands, bulk load batch index checking).
     *
     * @param req The current request.
     */
    public void processBatch(JdbcBulkLoadBatchRequest req)
        throws IgniteCheckedException {
        if (nextBatchIdx != req.batchIdx() && req.cmd() != CMD_FINISHED_ERROR)
            throw new IgniteSQLException("Batch #" + (nextBatchIdx + 1) +
                    " is missing. Received #" + req.batchIdx() + " instead.");

        nextBatchIdx++;

        switch (req.cmd()) {
            case CMD_FINISHED_EOF:
                processor.processBatch(req.data(), true);

                break;

            case CMD_CONTINUE:
                processor.processBatch(req.data(), false);

                break;

            case CMD_FINISHED_ERROR:
                break;

            default:
                throw new IgniteIllegalStateException("Command was not recognized: " + req.cmd());
        }
    }

    /**
     * Closes the underlying objects.
     * Currently we don't handle normal termination vs. abort.
     */
    @Override public void close() throws IOException {
        try {
            processor.close();

            nextBatchIdx = -1;
        }
        catch (Exception e) {
            throw new IOException("Unable to close processor: " + e.getMessage(), e);
        }
    }

    /**
     * Gets notified if current bulk load failed.
     *
     * @param reason reason why it failed.
     */
    public void onFail(Exception reason) {
        processor.onError(reason);
    }

    /**
     * Provides update counter for sending in the {@link JdbcBatchExecuteResult}.
     *
     * @return The update counter for sending in {@link JdbcBatchExecuteResult}.
     */
    public long updateCnt() {
        return processor.outputStreamer().updateCnt();
    }
}
