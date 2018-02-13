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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Bulk load (COPY) command processor used on server to keep various context data and process portions of input
 * received from the client side.
 */
public class BulkLoadCachePutAllProcessor extends BulkLoadProcessor {
    /** The cache to put data to. */
    private final GridCacheAdapter cache;

    /**
     * Creates bulk load processor.
     *
     * @param inputParser Parser of the input bytes.
     * @param dataConverter Converter, which transforms the list of strings parsed from the input stream to the
     *     key+value entry to add to the cache.
     * @param cache The cache to put data to.
     */
    public BulkLoadCachePutAllProcessor(BulkLoadParser inputParser,
        IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter, GridCacheAdapter cache) {
        super(inputParser, dataConverter);

        this.cache = cache;
    }

    /**
     * Processes the incoming batch and writes data to the cache by calling the data converter and output streamer.
     *
     * @param batchData Data from the current batch.
     * @param isLastBatch true if this is the last batch.
     * @throws IgniteIllegalStateException when called after {@link #close()}.
     */
    public void processBatch(byte[] batchData, boolean isLastBatch) throws IgniteCheckedException {
        List<List<Object>> inputRecords = inputParser.parseBatch(batchData, isLastBatch);

        if (inputRecords.isEmpty())
            return;

        Map<Object, Object> outRecs = new HashMap<>();

        for (List<Object> record : inputRecords) {
            IgniteBiTuple<?, ?> kv = dataConverter.apply(record);

            outRecs.put(kv.getKey(), kv.getValue());
        }

        cache.putAll(outRecs);

        updateCnt += outRecs.size();
    }

    /**
     * Aborts processing and closes the underlying objects ({@link IgniteDataStreamer}).
     */
    @Override public void close() {
        // no-op
    }
}
