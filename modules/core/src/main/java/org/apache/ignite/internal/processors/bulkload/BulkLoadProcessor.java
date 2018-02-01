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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

/**
 * Bulk load (COPY) command processor used on server to keep various context data and process portions of input
 * received from the client side.
 */
public class BulkLoadProcessor {
    /** Parameters extracted from the SQL COPY command. */
    private final BulkLoadParameters params;

    /** Parser of the input bytes. */
    private final BulkLoadParser inputParser;

    /** Converter, which transforms the list of strings parsed from the input stream to the key+value entry to add to
     * the cache. */
    private final IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter;

    /** Streamer that puts actual key/value into the cache. */
    private final BulkLoadCacheWriter outputStreamer;

    /**
     * Creates bulk load processor.
     *
     * @param params Parameters extracted from the SQL COPY command.
     * @param inputParser Parser of the input bytes.
     * @param dataConverter Converter, which transforms the list of strings parsed from the input stream to the
     *     key+value entry to add to the cache.
     * @param outputStreamer Streamer that puts actual key/value into the cache.
     */
    public BulkLoadProcessor(BulkLoadParameters params, BulkLoadParser inputParser,
        IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter, BulkLoadCacheWriter outputStreamer) {
        this.params = params;
        this.inputParser = inputParser;
        this.dataConverter = dataConverter;
        this.outputStreamer = outputStreamer;
    }

    /**
     * Returns the parameters extracted from the SQL COPY command.
     *
     * @return The parameters extracted from the SQL COPY command.
     */
    public BulkLoadParameters params() {
        return params;
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
     * Closes the underlying objects ({@link IgniteDataStreamer}).
     */
    public void close() {
        outputStreamer.close();
    }

    /**
     * Processes the incoming batch and writes data to the cache by calling the data converter and output streamer.
     *
     * @param batchData Data from the current batch.
     * @param isLastBatch true if this is the last batch.
     */
    public void processBatch(byte[] batchData, boolean isLastBatch) throws IgniteCheckedException {
        Iterable<List<Object>> inputRecords = inputParser.parseBatch(batchData, isLastBatch);

        for (List<Object> record : inputRecords) {
            IgniteBiTuple<?, ?> kv = dataConverter.apply(record);

            outputStreamer.accept(kv);
        }
    }

    public void abortProcessing() {
        // Currently does nothing. Will be used for rolling back a transaction.
    }
}
