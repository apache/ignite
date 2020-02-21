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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Bulk load (COPY) command processor used on server to keep various context data and process portions of input
 * received from the client side.
 */
public class BulkLoadProcessor implements AutoCloseable {
    /** Parser of the input bytes. */
    private final BulkLoadParser inputParser;

    /**
     * Converter, which transforms the list of strings parsed from the input stream to the key+value entry to add to
     * the cache.
     */
    private final IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter;

    /** Streamer that puts actual key/value into the cache. */
    private final BulkLoadCacheWriter outputStreamer;

    /** Becomes true after {@link #close()} method is called. */
    private boolean isClosed;

    /** Running query manager. */
    private final RunningQueryManager runningQryMgr;

    /** Query id. */
    private final Long qryId;

    /** Exception, current load process ended with, or {@code null} if in progress or if succeded. */
    private Exception failReason;

    /**
     * Creates bulk load processor.
     *
     * @param inputParser Parser of the input bytes.
     * @param dataConverter Converter, which transforms the list of strings parsed from the input stream to the
     *     key+value entry to add to the cache.
     * @param outputStreamer Streamer that puts actual key/value into the cache.
     * @param runningQryMgr Running query manager.
     * @param qryId Running query id.
     */
    public BulkLoadProcessor(BulkLoadParser inputParser, IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter,
        BulkLoadCacheWriter outputStreamer, RunningQueryManager runningQryMgr, Long qryId) {
        this.inputParser = inputParser;
        this.dataConverter = dataConverter;
        this.outputStreamer = outputStreamer;
        this.runningQryMgr = runningQryMgr;
        this.qryId = qryId;
        isClosed = false;
    }

    /**
     * Returns the streamer that puts actual key/value into the cache.
     *
     * @return Streamer that puts actual key/value into the cache.
     */
    public BulkLoadCacheWriter outputStreamer() {
        return outputStreamer;
    }

    /**
     * Processes the incoming batch and writes data to the cache by calling the data converter and output streamer.
     *
     * @param batchData Data from the current batch.
     * @param isLastBatch true if this is the last batch.
     * @throws IgniteIllegalStateException when called after {@link #close()}.
     */
    public void processBatch(byte[] batchData, boolean isLastBatch) throws IgniteCheckedException {
        if (isClosed)
            throw new IgniteIllegalStateException("Attempt to process a batch on a closed BulkLoadProcessor");

        Iterable<List<Object>> inputRecords = inputParser.parseBatch(batchData, isLastBatch);

        for (List<Object> record : inputRecords) {
            IgniteBiTuple<?, ?> kv = dataConverter.apply(record);

            outputStreamer.apply(kv);
        }
    }

    /**
     * Is called to notify processor, that bulk load execution, this processor is performing, failed with specified
     * exception.
     *
     * @param failReason why current load failed.
     */
    public void onError(Exception failReason) {
        this.failReason = failReason;
    }

    /**
     * Aborts processing and closes the underlying objects ({@link IgniteDataStreamer}).
     */
    @Override public void close() throws Exception {
        if (isClosed)
            return;

        try {
            isClosed = true;

            outputStreamer.close();
        }
        catch (Exception e) {
            if (failReason == null)
                failReason = e;

            throw e;
        }
        finally {
            runningQryMgr.unregister(qryId, failReason);
        }
    }
}
