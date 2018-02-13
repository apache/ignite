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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javafx.util.Pair;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

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
    private final GridCacheAdapter cache;

    /** Becomes true after {@link #close()} method is called. */
    private boolean isClosed;

    /**
     * Creates bulk load processor.
     *  @param inputParser Parser of the input bytes.
     * @param dataConverter Converter, which transforms the list of strings parsed from the input stream to the
     *     key+value entry to add to the cache.
     * @param outputStreamer Streamer that puts actual key/value into the cache.
     * @param cache
     */
    public BulkLoadProcessor(BulkLoadParser inputParser, IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter,
        BulkLoadCacheWriter outputStreamer, GridCacheAdapter cache) {
        this.inputParser = inputParser;
        this.dataConverter = dataConverter;
        this.outputStreamer = outputStreamer;
        isClosed = false;
        this.cache = cache;
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

        List<List<Object>> inputRecords = inputParser.parseBatch(batchData, isLastBatch);
//        Map<Object, Object> outRecs = new HashMap<>();

        if (inputRecords.isEmpty())
            return;

        Thread[] threads = new Thread[8];

        int strip = (inputRecords.size() + threads.length - 1) / threads.length;
        int start = 0;
        int end = start + strip;
        for (int i = 0; i < threads.length; ++i) {
            if (end > inputRecords.size())
                end = inputRecords.size();

            List<List<Object>> subList = inputRecords.subList(start, end);

            threads[i] = new Thread(new Runnable() {
                @Override public void run() {
                    for (List<Object> entry : subList) {
                        IgniteBiTuple<?, ?> kv = dataConverter.apply(entry);
                        outputStreamer.apply(kv);
                    }
                }
            });
            threads[i].start();

            start = end;
            end += strip;
        }

//        for (List<Object> record : inputRecords) {
//            noOpt2 = record;
//            IgniteBiTuple<?, ?> kv = dataConverter.apply(record);
//            outRecs.put(kv.getKey(), kv.getValue());

//            noOpt1 = kv;
//            outputStreamer.apply(kv);
//        }
//        cache.putAll(outRecs);

        for (int i = 0; i < threads.length; ++i) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                // swallow
            }
        }
    }

    public volatile IgniteBiTuple<?, ?> noOpt1;
    public volatile List<Object> noOpt2;

    /**
     * Aborts processing and closes the underlying objects ({@link IgniteDataStreamer}).
     */
    @Override public void close() throws Exception {
        if (isClosed)
            return;

        isClosed = true;

        outputStreamer.close();
    }
}
