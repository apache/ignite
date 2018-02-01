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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_CONTINUE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_EOF;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest.CMD_FINISHED_ERROR;

public class JdbcBulkLoadProcessor implements Closeable {
    /** A core processor that handles incoming data packets. */
    private final BulkLoadProcessor processor;

    /** Next batch index (for a very simple check that all batches were delivered to us). */
    protected long nextBatchIdx;

    /** FIXME SHQ */
    public JdbcBulkLoadProcessor(BulkLoadProcessor processor) {
        this.processor = processor;
        nextBatchIdx = 0;
    }

    /**
     * Processes a batch of input data. Context and request are supplied as parameters.
     * Returns a list of records parsed (in most cases this is a list of strings).
     *
     * <p>Note that conversion between parsed and database table type is done by the other
     * object (see {@link BulkLoadProcessor#dataConverter}) by the request processing code.
     * This method is not obliged to do this conversion.
     *
     * @param req The current request.
     * @return The list of records.
     * @throws IgniteCheckedException If any processing error occurs.
     */
    public void processBatch(JdbcBulkLoadBatchRequest req)
        throws IgniteCheckedException {
        if (nextBatchIdx != req.batchIdx())
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
                processor.abortProcessing();

                break;

            default:
                throw new IgniteIllegalStateException("Command was not recognized: " + req.cmd());
        }
    }

    /** FIXME SHQ */
    @Override public void close() {
        processor.close();
    }

    /** FIXME SHQ */
    public long updateCnt() {
        return processor.outputStreamer().updateCnt();

    }
}
