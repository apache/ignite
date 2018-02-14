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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Bulk load (COPY) command processor used on server to keep various context data and process portions of input
 * received from the client side.
 */
public class BulkLoadStreamerProcessor extends BulkLoadProcessor {
    /** Typical number of parallel processing threads per batch. */
    private static final int STREAMER_THREADS = 8;

    /** Max. number of parallel processing threads. */
    private static final int POOL_THREADS_CNT = STREAMER_THREADS * 2;

    /** Minimum of records per thread. */
    private static final int MIN_STRIP_SIZE = 32;

    /** Streamer that puts actual key/value into the cache. */
    private final IgniteDataStreamer<Object, Object> outputStreamer;

    /** Thread pool to execute processing threads. */
    private final ExecutorService threadPool;

    /** List of started convert+stream threads. */
    private List<Future<Integer>> futures;

    /**
     * Creates bulk load processor.
     *
     * @param inputParser Parser of the input bytes.
     * @param dataConverter Converter, which transforms the list of strings parsed from the input stream to the
     *     key+value entry to add to the cache.
     * @param outputStreamer Streamer that puts actual key/value into the cache.
     */
    public BulkLoadStreamerProcessor(BulkLoadParser inputParser, IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter,
        IgniteDataStreamer<Object, Object> outputStreamer) {
        super(inputParser, dataConverter);

        this.outputStreamer = outputStreamer;

        threadPool = Executors.newFixedThreadPool(POOL_THREADS_CNT);

        futures = new LinkedList<>();
    }

    /**
     * Processes the incoming batch and writes data to the cache by calling the data converter and output streamer.
     *
     * @param batchData Data from the current batch.
     * @param isLastBatch true if this is the last batch.
     * @throws IgniteIllegalStateException when called after {@link #close()}.
     */
    @Override public void processBatch(byte[] batchData, boolean isLastBatch) throws IgniteCheckedException {
        List<List<Object>> inputRecords = inputParser.parseBatch(batchData, isLastBatch);

        if (!inputRecords.isEmpty()) {
            int strip = Math.max(MIN_STRIP_SIZE, (inputRecords.size() + STREAMER_THREADS - 1) / STREAMER_THREADS);

            int start = 0;

            do {
                int end = start + strip;

                if (end > inputRecords.size())
                    end = inputRecords.size();

                final List<List<Object>> threadRecords = inputRecords.subList(start, end);

                Callable<Integer> streamer = new Callable<Integer>() {
                    @Override public Integer call() {
                        Map<Object, Object> dataToAdd = new HashMap<>(threadRecords.size());

                        for (List<Object> entry : threadRecords) {
                            IgniteBiTuple<?, ?> kv = dataConverter.apply(entry);

                            dataToAdd.put(kv.getKey(), kv.getValue());

                            if (Thread.interrupted())
                                return 0;
                        }

                        outputStreamer.addData(dataToAdd);
                        return dataToAdd.size();
                    }
                };

                futures.add(threadPool.submit(streamer));

                start = end;
            }
            while (start < inputRecords.size());
        }

        Iterator<Future<Integer>> futIt = futures.iterator();

        while (futIt.hasNext()) {
            Future<Integer> fut = futIt.next();

            if (isLastBatch || fut.isDone()) {
                try {
                    updateCnt += fut.get();
                    futIt.remove();
                }
                catch (InterruptedException ignored) {
                    return;
                }
                catch (ExecutionException e) {
                    futIt.remove();

                    throw new IgniteCheckedException(e.getCause());
                }
            }
        }
    }

    /**
     * Aborts processing and closes the underlying objects ({@link IgniteDataStreamer}).
     */
    @Override public void close() throws Exception {
        threadPool.shutdownNow();

        futures.clear();

        outputStreamer.close();
    }
}
